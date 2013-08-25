# vim: set filetype=javascript ts=2 et sw=2: */
# Workflow:
# Create the current state object
#
# for row containing latest state data (fields from bugs table record, fields from other tables (i.e. attachments, dependencies)
# Update the current state object with the latest field values
#
# Walk backward through activity records from bugs_activity (and other activity type tables). for set of activities:
# Create a new bug version object with the meta data about this activity
# Set id based on modification time
# *       Set valid_from field as modification time
# *       Set valid_to field as the modification time of the later version - 1 second
# Add modification data (who, when, what)
# For single value fields (i.e. assigned_to, status):
# Update the original state object by replacing the field value with the contents of the activities "removed" column
# For multi-value fields (i.e. blocks, CC, attachments):
# If a deletion, update the original state object by adding the value from the "removed" column to the field values array.
# If an addition, find and remove the added item from the original state object
#
# When finished with all activities, the current state object should reflect the original state of the bug when created.
# Now, build the full state of each intermediate version of the bug.
#
# for bug version object that was created above:
# Merge the current state object into this version object
# Update fields according to the modification data
#
# When doing an incremental update (ie. with START_TIME specified), Look at any bug that has been modified since the
# cutoff time, and build all versions.  Only index versions after START_TIME in ElasticSearch.


# Used to split a flag into (type, status [,requestee])
# Example: "review?(mreid@mozilla.com)" -> (review, ?, mreid@mozilla.com)
# Example: "review-" -> (review, -)
import re
import math

from util.cnv import CNV
from util.debug import D
from util.query import Q
from util.struct import Struct
from util.files import File
from util.maths import Math


FLAG_PATTERN = re.compile("^(.*)([?+-])(\\([^)]*\\))?$")

# Used to reformat incoming dates into the expected form.
# Example match: "2012/01/01 00:00:00.000"
DATE_PATTERN_STRICT = re.compile("^[0-9]{4}[\\/-][0-9]{2}[\\/-][0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}")
# Example match: "2012-08-08 0:00"
DATE_PATTERN_RELAXED = re.compile("^[0-9]{4}[\\/-][0-9]{2}[\\/-][0-9]{2}")

# Fields that could have been truncated per bug 55161
TRUNC_FIELDS = ["cc", "blocked", "dependson", "keywords"]
NUMERIC_FIELDS=["dependson", "blocked", "dupe_by", "dupe_of"]


class parse_bug_history_():

    def __init__(self, settings, output_queue):
        self.bzAliases = None
        self.startNewBug(Struct(**{"bug_id":0, "modified_ts":0, "_merge_order":1}))
        self.prevActivityID = None
        self.prev_row=None
        self.settings=settings
        self.output = output_queue

        self.initializeAliases()
        

    def processRow(self, row_in):
        if len(row_in.items())==0: return 
        # row_in.bug_id, row_in.modified_ts, row_in.modified_by, row_in.field_name, row_in.field_value, row_in.field_value_removed, row_in.attach_id, row_in._merge_order):
        try:
            self.currBugID = row_in.bug_id
#            if row_in.bug_id==1883:
#                D.println("")
#            if row_in.field_name=="attachments.isobsolete":
#                D.println("error")

            if self.settings.debug: D.println("process row: ${row}", {"row":row_in})

            # For debugging purposes:
            if self.settings.END_TIME > 0 and row_in.modified_ts > self.settings.END_TIME:
                D.println("Skipping change after END_TIME (" + self.settings.END_TIME + ")")
                return

#            if self.currBugState.bug_id!=0 and row_in._merge_order>1 and self.currBugState.created_ts is None:
#                D.println("expecting a created_ts")
            if row_in.field_value=="None":
                D.println("not expected")


            # If we have switched to a new bug
            if self.prevBugID < self.currBugID:
                # Start replaying versions in ascending order to build full data on each version
                D.println("Emitting intermediate versions for ${bug_id}", {"bug_id":self.prevBugID})
                self.populateIntermediateVersionObjects()
                self.startNewBug(row_in)


            # Bugzilla bug workaround - some values were truncated, introducing uncertainty / errors:
            # https://bugzilla.mozilla.org/show_bug.cgi?id=55161
            if row_in.field_name in TRUNC_FIELDS:
                row_in.field_value=CNV.value2string(row_in.field_value)
                row_in.field_value_removed=CNV.value2string(row_in.field_value_removed)
                uncertain = False

                # Unknown value extracted from a possibly truncated field
                if row_in.field_value == "? ?" or row_in.field_value_removed == "? ?":
                    uncertain = True
                    if row_in.field_value == "? ?":
                        D.println("Encountered uncertain added value.  Skipping.")
                        row_in.field_value = None

                    if row_in.field_value_removed == "? ?":
                        D.println("Encountered uncertain removed value.  Skipping.")
                        row_in.field_value_removed = None


                # Possibly truncated value extracted from a possibly truncated field
                if row_in.field_value is not None and row_in.field_value.startswith("? "):
                    uncertain = True
                    row_in.field_value = row_in.field_value[2:]

                if row_in.field_value_removed is not None and row_in.field_value_removed.startswith("? "):
                    uncertain = True
                    row_in.field_value_removed = row_in.field_value_removed[2:]

                if uncertain:
                    # Process the "uncertain" flag as an activity
                    D.println("Setting this bug to be uncertain.")
                    self.processBugsActivitiesTableItem(Struct(**{
                        "modified_ts": row_in.modified_ts,
                        "modified_by": row_in.modified_by,
                        "field_name":"uncertain",
                        "field_value":"1",
                        "field_value_removed":None,
                        "attach_id":None
                    }))
                    if row_in.field_value is None and row_in.field_value_removed is None:
                        D.println("Nothing added or removed. Skipping update.")
                        return

            if self.currBugID < 999999999:
                # Treat timestamps as int values
                field_value = row_in.field_value if row_in.field_name.endswith("_ts") else row_in.field_value
#                if self.currBugState.bug_id==1883:
#                    D.println("")
                # Determine where we are in the bug processing workflow
                if row_in._merge_order==1:
                    self.processSingleValueTableItem(row_in.field_name, field_value)
                elif row_in._merge_order==2:
                    self.processMultiValueTableItem(row_in.field_name, field_value)
                elif row_in._merge_order==7:
                    self.processAttachmentsTableItem(row_in)
                elif row_in._merge_order==8:
                    self.processFlagsTableItem(row_in)
                elif row_in._merge_order==9:
                    self.processBugsActivitiesTableItem(row_in)
                else:
                    D.warning("Unhandled merge_order: '" + row_in._merge_order + "'")

        except Exception, e:
            D.warning("Problem processing row: ${row}", {"row":row_in}, e)
        finally:
            self.prev_row=row_in
#            if self.currBugState.bug_id!=0 and row_in._merge_order>1 and self.currBugState.created_ts is None:
#                D.println("expecting a created_ts")

    @staticmethod
    def uid(bug_id, modified_ts):
        return str(bug_id) + "_" + str(modified_ts)[0:-3]

    def startNewBug(self, row_in):
        self.prevBugID = row_in.bug_id
        self.bugVersions = []
        self.bugVersionsMap =Struct()
        self.currActivity =Struct()
        self.currBugAttachmentsMap=Struct()
        self.currBugState = Struct(
            _id=parse_bug_history_.uid(row_in.bug_id, row_in.modified_ts),
            bug_id=row_in.bug_id,
            modified_ts= row_in.modified_ts,
            modified_by= row_in.modified_by,
            reported_by= row_in.modified_by,
            attachments= [],
            flags=[],
            cc=set([]),
            blocked=set([]),
            dependson=set([]),
            keywords=set([]),
            dupe_by=set([]),
            dupe_of=set([])
        )

        if row_in._merge_order != 1:
            # Problem: No entry found in the 'bugs' table.
            D.warning("Current bugs table record not found for bug_id: ${bug_id}  (merge order should have been 1, but was ${_merge_order})", row_in)


    def processSingleValueTableItem(self, field_name, field_value):
        self.currBugState[field_name] = field_value

    def processMultiValueTableItem(self, field_name, field_value):
        if self.currBugState[field_name] is None:
            self.currBugState[field_name] = set()  #SHOULD NEVER HAPPEN

        try:
            self.currBugState[field_name].add(field_value)
            if "None" in self.currBugState[field_name]:
                D.println("error")
            return None
        except Exception, e:
            D.warning("Unable to push ${value} to array field ${field} on bug ${bug_id} current value: ${curr_value}",{
                "value":field_value,
                "field":field_name,
                "bug_id":self.currBugID,
                "curr_value":self.currBugState[field_name]
            }, e)


    def processAttachmentsTableItem(self, row_in):
        currActivityID = parse_bug_history_.uid(self.currBugID, row_in.modified_ts)
        if currActivityID != self.prevActivityID:
            self.currActivity =Struct(
                _id=currActivityID,
                modified_ts=row_in.modified_ts,
                modified_by= row_in.modified_by,
                changes= []
            )

            self.bugVersions.append(self.currActivity)
            self.bugVersionsMap[currActivityID] = self.currActivity
            self.prevActivityID = currActivityID
            self.currActivity.changes.append(Struct(
                field_name="attachment_added",
                attach_id=row_in.attach_id
            ))

        att=self.currBugAttachmentsMap[str(row_in.attach_id)]
        if att is None:
            att={
                "attach_id": row_in.attach_id,
                "modified_ts": row_in.modified_ts,
                "created_ts": row_in.created_ts,
                "modified_by": row_in.modified_by,
                "flags": []
            }
            self.currBugAttachmentsMap[str(row_in.attach_id)]=att

        att["created_ts"]=Math.min(row_in.modified_ts, att["created_ts"])
        if row_in.field_name=="created_ts" and row_in.field_value is None:
            pass
        else:
            att[row_in.field_name] = row_in.field_value

    def processFlagsTableItem(self, row_in):
        flag = self.makeFlag(row_in.field_value, row_in)
        if row_in.attach_id is not None:
            if self.currBugAttachmentsMap[str(row_in.attach_id)] is None:
                D.println("Unable to find attachment ${attach_id} for bug_id ${bug_id}", {
                    "attach_id":row_in.attach_id,
                    "bug_id":self.currBugID
                })

            self.currBugAttachmentsMap[str(row_in.attach_id)].flags.append(flag)
        else:
            self.currBugState.flags.append(flag)


    def processBugsActivitiesTableItem(self, row_in):
        if self.currBugState.created_ts is None:
            D.error("must has created_ts")

        if row_in.field_name == "flagtypes_name":
            row_in.field_name = "flags"

        multi_field_value = self.getMultiFieldValue(row_in.field_name, row_in.field_value)
        multi_field_value_removed = parse_bug_history_.getMultiFieldValue(row_in.field_name, row_in.field_value_removed)

        currActivityID = parse_bug_history_.uid(self.currBugID, row_in.modified_ts)
        if currActivityID != self.prevActivityID:
            self.currActivity = self.bugVersionsMap[currActivityID]
            if self.currActivity is None:
                self.currActivity =Struct(
                    _id= currActivityID,
                    modified_ts= row_in.modified_ts,
                    modified_by= row_in.modified_by,
                    changes= []
                )
                self.bugVersions.append(self.currActivity)

            self.prevActivityID = currActivityID

        self.currActivity.changes.append({
            "field_name": row_in.field_name,
            "field_value": row_in.field_value,
            "field_value_removed": row_in.field_value_removed,
            "attach_id": row_in.attach_id
        })
        if row_in.attach_id is not None:
            attachment = self.currBugAttachmentsMap[str(row_in.attach_id)]
            if attachment is None:
                #we are going backwards in time, no need to worry about these?  maybe delete this change for public bugs
                D.println("Unable to find attachment ${attach_id} for bug_id ${bug_id}: ${attachments}", {
                    "attach_id":row_in.attach_id,
                    "bug_id":self.currBugID,
                    "attachments":self.currBugAttachmentsMap
                })
            else:
                if isinstance(attachment[row_in.field_name], set):
                    a = attachment[row_in.field_name]
                    # Can have both added and removed values.
                    a=self.removeValues(a, multi_field_value, "added", row_in.field_name, "attachment", attachment)
                    a=self.addValues(a, multi_field_value_removed, "removed attachment", row_in.field_name, self.currActivity)
                    attachment[row_in.field_name]=a
                elif row_in.field_name=="flags":
                    self.processFlagChange(attachment, row_in, row_in.modified_ts, row_in.modified_by, reverse=True)
                else:
                    attachment[row_in.field_name] = row_in.field_value_removed


        else:
            if isinstance(self.currBugState[row_in.field_name], set):
                # PROBLEM: WHEN GOING BACK IN HISTORY, AND THE ADDED VALUE IS NOT FOUND IN THE CURRENT
                # STATE, IT IS STILL RECORDED (see above self.currActivity.changes.append...).  THIS MEANS
                # WHEN GOING THROUGH THE CHANGES IN ORDER THE VALUE WILL EXIST, BUT IT SHOULD NOT
                a = self.currBugState[row_in.field_name]
                # Can have both added and removed values.
                a = self.removeValues(a, multi_field_value, "added", row_in.field_name, "currBugState", self.currBugState)
                a = self.addValues(a, multi_field_value_removed, "removed bug", row_in.field_name, self.currActivity)
                self.currBugState[row_in.field_name]=a
                if a is None:
                    D.println("error")
            elif row_in.field_name=="flags":
                self.processFlagChange(self.currBugState, row_in, row_in.modified_ts, row_in.modified_by, reverse=True)
            elif parse_bug_history_.isMultiField(row_in.field_name):
                # field must currently be missing, otherwise it would
                # be an instanceof Array above.  This handles multi-valued
                # fields that are not first processed by processMultiValueTableItem().
                self.currBugState[row_in.field_name].add(multi_field_value_removed)
                if "None" in self.currBugState[row_in.field_name]:
                    D.println("error")
            else:
                # Replace current value
                self.currBugState[row_in.field_name] = row_in.field_value_removed


    @staticmethod
    def sortAscByField(a, b, aField):
        if a[aField] > b[aField]:
            return 1
        if a[aField] < b[aField]:
            return -1
        return 0

    @staticmethod
    def sortDescByField(a, b, aField):
        return -1 * parse_bug_history_.sortAscByField(a, b, aField)

    
    def populateIntermediateVersionObjects(self):
        # Make sure the self.bugVersions are in ascending order by modification time.
        # They could be mixed because of attachment activity
        self.bugVersions=Q.sort(self.bugVersions, {"field":"modified_ts", "sort":1})
#
#        if self.currBugState.bug_id!=0 and self.currBugState.created_ts is None:
#            D.println("expecting a created_ts")
#        for id, att in self.currBugAttachmentsMap.items():
#            if att["created_ts"] is None:
#                D.println("expecting a created_ts")
                
        # Tracks the previous distinct value for field
        prevValues ={}
        currVersion=None
        # Prime the while loop with an empty next version so our first iteration outputs the initial bug state
        nextVersion=Struct(_id=self.currBugState._id, changes=[])

        

        flagMap ={}
        # A monotonically increasing version number (useful for debugging)
        self.currBugVersion = 1

        # continue if there are more bug versions, or there is one final nextVersion
        while len(self.bugVersions) > 0 or nextVersion:
            try:
                currVersion = nextVersion
                if len(self.bugVersions) > 0:
                    nextVersion = self.bugVersions.pop() # Oldest version
                else:
                    nextVersion = None

                D.println("Populating JSON for version " + currVersion._id)


#                if self.currBugState.bug_id!=0 and self.currBugState.created_ts is None:
#                    D.println("expecting a created_ts")
#                for id, att in self.currBugAttachmentsMap.items():
#                    if att["created_ts"] is None:
#                        D.println("expecting a created_ts")

                # Decide whether to merge this bug activity into the current state (without emitting
                # a separate JSON document). This addresses the case where an attachment is created
                # at exactly the same time as the bug itself.
                # Effectively, we combine all the changes for a given timestamp into the last one.
                mergeBugVersion = False
                if nextVersion is not None and currVersion._id == nextVersion._id:
                    D.println("Merge mode: activated " + self.currBugState._id)
                    mergeBugVersion = True

                # Link this version to the next one (if there is a next one)
                if nextVersion is not None:
                    D.println("We have a nextVersion: ${timestamp} (ver ${next_version})", {
                        "timestamp":nextVersion.modified_ts,
                        "next_version":self.currBugVersion + 1
                    })
                    self.currBugState.expires_on = nextVersion.modified_ts
                else:
                    # Otherwise, we don't know when the version expires.
                    D.println("We have no nextVersion after #${version}", {"version": self.currBugVersion})
                    #                                921878882000
                    self.currBugState.expires_on = None

                # Copy all attributes from the current version into self.currBugState
                for propName, propValue in currVersion.items():
                    self.currBugState[propName] = propValue

                # Now walk self.currBugState forward in time by applying the changes from currVersion
                changes = currVersion.changes
                #D.println("Processing changes: "+CNV.object2JSON(changes))
                for change in changes:

#                    if self.currBugState.bug_id!=0 and self.currBugState.created_ts is None:
#                        D.println("expecting a created_ts")
#                    for id, att in self.currBugAttachmentsMap.items():
#                        if att["created_ts"] is None:
#                            D.println("expecting a created_ts")



                    D.println("Processing change: " + CNV.object2JSON(change))
                    target = self.currBugState
                    targetName = "currBugState"
                    attachID = change["attach_id"]
                    if attachID is not None:
                        # Handle the special change record that signals the creation of the attachment
                        if change.field_name == "attachment_added":
                            # This change only exists when the attachment has been added to the map, so no missing case needed.
                            att=self.currBugAttachmentsMap[str(attachID)]
                            self.currBugState.attachments.append(att)
                            continue
                        else:
                            # Attachment change
                            target = self.currBugAttachmentsMap[str(attachID)]
                            targetName = "attachment"
                            if target is None:
                                D.warning("Encountered a change to missing attachment for bug '"
                                    + currVersion["bug_id"] + "': " + CNV.object2JSON(change) + ".")

                                # treat it as a change to the main bug instead :(
                                target = self.currBugState
                                targetName = "currBugState"



                    # Track the previous value
                    if not parse_bug_history_.isMultiField(change.field_name):
                        # Single-value field has changed in bug or attachment
                        # Make sure it's actually changing.  We seem to get change
                        #  entries for attachments that show the current field value.
                        if target[change.field_name] != change.field_value:
                            self.setPrevious(target, change.field_name, target[change.field_name], currVersion.modified_ts)
                        else:
                            D.println("Skipping fake change to " + targetName + ": "
                                + CNV.object2JSON(target) + ", change: " + CNV.object2JSON(change))

                    elif change.field_name == "flags":
                        self.processFlagChange(target, change, currVersion.modified_ts, currVersion.modified_by)
                    else:
                        D.println("Skipping previous_value for " + targetName
                            + " multi-value field " + change.field_name)

                    # Multi-value fields
                    if change.field_name == "flags":
                        # Already handled by "processFlagChange" above.
                        D.println("Skipping previously processed flag change")
                    elif isinstance(target[change.field_name], set):
                        a = target[change.field_name]
                        multi_field_value = parse_bug_history_.getMultiFieldValue(change.field_name, change.field_value)
                        multi_field_value_removed = parse_bug_history_.getMultiFieldValue(change.field_name,
                                                                                          change.field_value_removed)

                        # This was a deletion, find and delete the value(s)
                        a = self.removeValues(a, multi_field_value_removed, "removed", change.field_name, targetName, target)
                        # Handle addition(s) (if any)
                        a = self.addValues(a, multi_field_value, "added", change.field_name, currVersion)
                        target[change.field_name]=a
                    elif parse_bug_history_.isMultiField(change.field_name):
                        # First appearance of a multi-value field
                        target[change.field_name] = set([change.field_value])  #SHOULD NEVER HAPPEN
                    else:
                        # Simple field change.
                        target[change.field_name] = change.field_value


                # Do some processing to make sure that diffing betweens runs stays as similar as possible.
                parse_bug_history_.stabilize(self.currBugState)

                # Empty string breaks ES date parsing, remove it from bug state.
                for dateField in ["deadline", "cf_due_date", "cf_last_resolved"]:
                    # Special case to handle values that can't be parsed by ES:
                    if self.currBugState[dateField] == "":
                        # Skip empty strings
                        self.currBugState[dateField] = None


                # Also reformat some date fields
                for dateField in ["deadline", "cf_due_date"]:
                    if self.currBugState[dateField] is not None and DATE_PATTERN_RELAXED.match(self.currBugState[dateField]):
                        # Convert "2012/01/01 00:00:00.000" to "2012-01-01"
                        # Example: bug 643420 (deadline)
                        #          bug 726635 (cf_due_date)
                        self.currBugState[dateField] = self.currBugState[dateField].substring(0, 10).replace("/", '-')


                for dateField in ["cf_last_resolved"]:
                    if self.currBugState[dateField] is not None and DATE_PATTERN_STRICT.match(self.currBugState[dateField]):
                        # Convert "2012/01/01 00:00:00.000" to "2012-01-01T00:00:00.000Z", then to a timestamp.
                        # Example: bug 856732 (cf_last_resolved)
                        # dateString = self.currBugState[dateField].substring(0, 10).replace("/", '-') + "T" + self.currBugState[dateField].substring(11) + "Z"
                        self.currBugState[dateField] = CNV.datetime2unixmilli(CNV.string2datetime(self.currBugState[dateField]+"000", "%Y/%m/%d %H%M%S%f"))


                self.currBugState.bug_version_num = self.currBugVersion

                if mergeBugVersion is not None:
                    # This is not a "merge", so output a row for this bug version.
                    self.currBugVersion+=1
                    # Output this version if either it was modified after START_TIME, or if it
                    # expired after START_TIME (the latter will update the last known version of the bug
                    # that did not have a value for "expires_on").
                    if self.currBugState.modified_ts >= self.settings.START_TIME or self.currBugState.expires_on >= self.settings.START_TIME:
                        state=scrub(self.currBugState)
                        if state.blocked is not None and len(state.blocked)==1 and "None" in state.blocked:
                            D.println("error")
                        D.println("Bug ${bug_state.bug_id} v${bug_state.bug_version_num} (_id = ${bug_state._id}): ${bug_state}" , {
                            "bug_state":state
                        })
                        self.output.add(state)
                    else:
                        D.println("Not outputting ${-id} - it is before self.START_TIME (${start_time})", {
                            "_id":self.currBugState._id,
                            "start_time":self.settings.START_TIME
                        })

                else:
                    D.println("Merging a change with the same timestamp = ${bug_state._id}: ${bug_state}",{
                        "bug_state":currVersion
                    })
            finally:
#                if self.currBugState.bug_id!=0 and self.currBugState.created_ts is None:
#                    D.println("expecting a created_ts")
#                for id, att in self.currBugAttachmentsMap.items():
#                    if att["created_ts"] is None:
#                        D.println("expecting a created_ts")
                pass
            
    def findFlag(self, aFlagList, aFlag):
        existingFlag = self.findByKey(aFlagList, "value", aFlag.value)  # len([f for f in aFlagList if f.value==aFlag.value])>0   aFlag.value in Q.select(aFlagList, "value")
        if existingFlag is None:
            for eFlag in aFlagList:
                if (eFlag.request_type == aFlag.request_type
                    and eFlag.request_status == aFlag.request_status
                    and (self.bzAliases[aFlag.requestee + "=" + eFlag.requestee] # Try both directions.
                    or self.bzAliases[eFlag.requestee + "=" + aFlag.requestee])):
                    D.println("Using bzAliases to match change '" + aFlag.value + "' to '" + eFlag.value + "'")
                    existingFlag = eFlag
                    break
                    
        return existingFlag

    def processFlagChange(self, aTarget, aChange, aTimestamp, aModifiedBy, reverse=False):
        addedFlags = parse_bug_history_.getMultiFieldValue("flags", aChange.field_value)
        removedFlags = parse_bug_history_.getMultiFieldValue("flags", aChange.field_value_removed)

        #going in reverse when traveling through bugs backwards in time
        if reverse:
            (addedFlags, removedFlags)=(removedFlags, addedFlags)

        # First, mark any removed flags as straight-up deletions.
        for flagStr in removedFlags:
            if flagStr == "":
                continue

            flag = parse_bug_history_.makeFlag(flagStr, Struct(**{"modified_ts":aTimestamp, "modified_by":aModifiedBy}))
            existingFlag = self.findFlag(aTarget.flags, flag)

            if existingFlag is not None:
                # Carry forward some previous values:
                existingFlag["previous_modified_ts"] = existingFlag["modified_ts"]
                if existingFlag["modified_by"] != aModifiedBy:
                    existingFlag["previous_modified_by"] = existingFlag["modified_by"]
                    existingFlag["modified_by"] = aModifiedBy

                # Add changed stuff:
                existingFlag["modified_ts"] = aTimestamp
                existingFlag["previous_status"] = flag["request_status"]
                existingFlag["previous_value"] = flagStr
                existingFlag["request_status"] = "D"
                existingFlag["value"] = ""
                # request_type stays the same.
                # requestee stays the same.

                duration_ms = existingFlag["modified_ts"] - existingFlag["previous_modified_ts"]
                existingFlag["duration_days"] = math.floor(duration_ms / (1000.0 * 60 * 60 * 24))
            else:
                D.warning("Did not find a corresponding flag for removed value '"
                    + flagStr + "' in " + CNV.object2JSON(aTarget.flags))


        # See if we can align any of the added flags with previous deletions.
        # If so, try to match them up with a "dangling" removed flag
        for flagStr in addedFlags:
            if flagStr == "":
                continue

            flag = self.makeFlag(flagStr, Struct(**{"modified_ts":aTimestamp, "modified_by":aModifiedBy}))

            if aTarget.flags is None:
                D.println("Warning: processFlagChange called with unset 'flags'")
                aTarget.flags = []

            candidates = [element for element in aTarget.flags if
                element["value"] == ""
                    and flag["request_type"] == element["request_type"]
                    and flag["request_status"] != element["previous_status"] # Skip "r?(dre@mozilla)" -> "r?(mark@mozilla)"
            ]

            if len(candidates) > 0:
                chosen_one = candidates[0]
                if len(candidates) > 1:
                    # Multiple matches - use the best one.
                    D.println("Matched added flag ${flag} to multiple removed flags.  Using the best of these:\n", {
                        "flag":flag,
                        "candidates":candidates
                    })
                    matched_ts = [element for element in candidates if
                        flag.modified_ts == element.modified_ts
                    ]

                    if len(matched_ts) == 1:
                        D.println("Matching on modified_ts fixed it")
                        chosen_one = matched_ts[0]
                    else:
                        D.println("Matching on modified_ts left us with ${num} matches", {"num":len(matched_ts)})
                        # If we had no matches (or many matches), try matching on requestee.
                        matched_req = [element for element in candidates if
                            # Do case-insenitive comparison
                            element["requestee"] is not None and
                                flag["modified_by"].lower() == element["requestee"].lower()
                        ]
                        if len(matched_req) == 1:
                            D.println("Matching on requestee fixed it")
                            chosen_one = matched_req[0]
                        else:
                            D.warning("Matching on requestee left us with ${num} matches. Skipping match.", {"num":len(matched_req)})
                            # TODO: add "uncertain" flag?
                            chosen_one = None


                else:
                    # Obvious case - matched exactly one.
                    D.println("Matched added flag " + CNV.object2JSON(flag) + " to removed flag " + CNV.object2JSON(chosen_one))

                if chosen_one is not None:
                    for f in ["value", "request_status", "requestee"]:
                        if flag[f] is not None:
                            chosen_one[f] = flag[f]



                # We need to avoid later adding this flag twice, since we rolled an add into a delete.
            else:
                # No matching candidate. Totally new flag.
                D.println("Did not match added flag " + CNV.object2JSON(flag) + " to anything: " + CNV.object2JSON(aTarget.flags))
                aTarget.flags.append(flag)




    def setPrevious(self, dest, aFieldName, aValue, aChangeAway):
        if not dest["previous_values"]:
            dest["previous_values"] ={}

        pv = dest["previous_values"]
        vField = aFieldName + "_value"
        caField = aFieldName + "_change_away_ts"
        ctField = aFieldName + "_change_to_ts"
        ddField = aFieldName + "_duration_days"

        pv[vField] = aValue
        # If we have a previous change for this field, then use the
        # change-away time as the new change-to time.
        if pv[caField] is not None:
            pv[ctField] = pv[caField]
        else:
            # Otherwise, this is the first change for this field, so
            # use the creation timestamp.
            pv[ctField] = dest["created_ts"]

        pv[caField] = aChangeAway
        try:
            duration_ms = pv[caField] - pv[ctField]
        except Exception, e:
            D.error("", e)
        pv[ddField] = math.floor(duration_ms / (1000.0 * 60 * 60 * 24))

    @staticmethod
    def findByKey(aList, aField, aValue):
        for item in aList:
            if isinstance(item, basestring):
                D.error("expecting structure")
            if item[aField] == aValue:
                return item
        return None

    @staticmethod
    def stabilize(bug):
        if bug.cc is not None:
            bug.cc=list(bug.cc).sort()

        if bug.changes is not None:
            bug.changes=Q.sort(bug.changes, "field_name")


    @staticmethod
    def makeFlag(flag, row_in):
        flagParts = Struct(
            modified_ts=row_in.modified_ts,
            modified_by=row_in.modified_by,
            value=flag
        )

        matches = FLAG_PATTERN.match(flag)
        if matches:
            flagParts.request_type = matches.group(1)
            flagParts.request_status = matches.group(2)
            if matches.start(3)!=-1 and len(matches.group(3)) > 2:
                flagParts.requestee = matches.group(3)[1:-1]


        return flagParts

    @staticmethod
    def addValues(aSet, someValues, valueType, fieldName, anObj):
        if len(someValues)==0: return aSet
        D.println("Adding " + valueType + " " + fieldName + " values:" + CNV.object2JSON(someValues))
        if fieldName == "flags":
            return aSet.extend(someValues)
            ## TODO: Some bugs (like 685605) actually have duplicate flags. Do we want to keep them?
            #/*
            # # Check if this flag has already been incorporated into a removed flag. If so, don't add it again.
            # dupes = anArray.filter(def(element, index, array):
            # return element["value"] == added
            # and element["modified_by"] == anObj.modified_by
            # and element["modified_ts"] == anObj.modified_ts
            # })
            # if dupes and dupes.length > 0:
            # D.println("Skipping duplicated added flag '" + added + "' since info is already in " + CNV.object2JSON(dupes[0]))
            # else:
            # */        else:
        return aSet | someValues




    def removeValues(self, anArray, someValues, valueType, fieldName, arrayDesc, anObj):
        if fieldName == "flags":
            for v in someValues:
                len_ = len(anArray)
                flag = parse_bug_history_.makeFlag(v, Struct(**{"modified_ts":0, "modified_by":0}))
                for f in list(anArray):
                    # Match on complete flag (incl. status) and flag value
                    if f.value == v:
                        anArray.remove(f)
                        break
                    elif flag.requestee is not None:
                        # Match on flag type and status, then use Aliases to match
                        # TODO: Should we try exact matches all the way to the end first?
                        if (f.request_type == flag.request_type
                            and f.request_status == flag.request_status
                            and self.bzAliases[flag.requestee + "=" + f.requestee]):
                            D.println("Using bzAliases to match '" + v + "' to '" + f.value + "'")
                            anArray.remove(f)
                            break



                if len_ == len(anArray):
                    D.warning("Unable to find " + valueType + " flag " + fieldName + ":" + v
                        + " in " + arrayDesc + ": " + CNV.object2JSON(anObj))
                    
            return anArray
        else:
            diff=someValues-anArray
            if len(diff)>0:
                if fieldName=="cc" or fieldName=="keywords":
                    # Don't make too much noise about mismatched items.
                    D.println("Unable to find " + valueType  + " value " + fieldName + ":" + CNV.object2JSON(diff)
                        + " in " + arrayDesc + ": " + CNV.object2JSON(anObj))
                else:
                    D.warning("Unable to find " + valueType + " value " + fieldName + ":" + CNV.object2JSON(diff)
                        + " in " + arrayDesc + ": " + CNV.object2JSON(anObj))
            output=anArray-someValues
            if "None" in output:
                D.println("error")

            return output





    @staticmethod
    def isMultiField(name):
        return (name == "flags" or name == "cc" or name == "keywords"
            or name == "dependson" or name == "blocked" or name == "dupe_by"
            or name == "dupe_of" or name == "bug_group" or name == "see_also")

    @staticmethod
    def getMultiFieldValue(name, value):
        if value is None:
            return set()
        if parse_bug_history_.isMultiField(name):
            if name in NUMERIC_FIELDS:
                return set([int(s.strip()) for s in value.split(",")])
            else:
                return set([s.strip() for s in value.split(",")])

        return {value}

    
    def initializeAliases(self):
        try:
            BZ_ALIASES = File(self.settings.alias_file).read().split("\n")
            self.bzAliases ={}
            D.println("Initializing aliases")
            for alias in [s.split(";")[0].strip() for s in BZ_ALIASES]:
                if self.settings.debug: D.println("Adding alias '" + alias + "'")
                self.bzAliases[alias] = True
        except Exception, e:
            D.error("Can not init aliases", e)

            
#REMOVE KEYS OF DEGENERATE VALUES (EMPTY STRINGS, EMPTY LISTS, AND NULLS)
def scrub(r):
    return Struct(**_scrub(r))

def _scrub(r):
    try:
        if isinstance(r, dict):
            output={}
            for k, v in r.items():
                v=_scrub(v)
                if v is not None: output[k]=v
            if len(output)==0: return None
            return output
        elif hasattr(r, '__iter__'):
            output=[]
            for v in r:
                v=_scrub(v)
                if v is not None: output.append(v)
            if len(output)==0: return None
            return output
        elif r is None or r=="":
            return None
        else:
            return r
    except Exception, e:
        D.warning("Can not scrub: ${json}", {"json":r})






