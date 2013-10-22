################################################################################
## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this file,
## You can obtain one at http://mozilla.org/MPL/2.0/.
################################################################################
## PYTHON VERISON OF ../resources/javascript/parse_bug_history.js
################################################################################



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
# When doing an incremental update (ie. with start_time specified), Look at any bug that has been modified since the
# cutoff time, and build all versions.  Only index versions after start_time in ElasticSearch.


# Used to split a flag into (type, status [,requestee])
# Example: "review?(mreid@mozilla.com)" -> (review, ?, mreid@mozilla.com)
# Example: "review-" -> (review, -)
import re
import math
from bzETL.util import struct, strings
from bzETL.util.struct import nvl
from bzETL.util.multiset import Multiset
from transform_bugzilla import normalize, NUMERIC_FIELDS, MULTI_FIELDS

from bzETL.util.cnv import CNV
from bzETL.util.logs import Log
from bzETL.util.query import Q
from bzETL.util.struct import Struct, Null
from bzETL.util.files import File
from bzETL.util.maths import Math


FLAG_PATTERN = re.compile("^(.*)([?+-])(\\([^)]*\\))?$")


DEBUG_CHANGES = False   # SHOW ACTIVITY RECORDS BEING PROCESSED
DEBUG_STATUS = False    # SHOW CURRENT STATE OF PROCESSING


# Fields that could have been truncated per bug 55161
TRUNC_FIELDS = ["cc", "blocked", "dependson", "keywords"]

STOP_BUG = 999999999
MAX_TIME = 9999999999000

class parse_bug_history_():

    def __init__(self, settings, output_queue):
        self.aliases = Null
        self.startNewBug(struct.wrap({"bug_id":0, "modified_ts":0, "_merge_order":1}))
        self.prevActivityID = Null
        self.prev_row=Null
        self.settings=settings
        self.output = output_queue

        self.initializeAliases()
        

    def processRow(self, row_in):
        if len(row_in.items())==0: return 
        try:
            self.currBugID = row_in.bug_id
            # if self.currBugState.created_ts == Null:
            #     Log.note("PROBLEM expecting a created_ts (did you install the timezone database into your MySQL instance?)")

            if self.settings.debug: Log.note("process row: {{row}}", {"row":row_in})

            # For debugging purposes:
            if self.settings.end_time > 0 and row_in.modified_ts > self.settings.end_time:
                Log.note("Skipping change {{modified_ts}} > end_time={{end_time}}", {
                    "end_time":self.settings.end_time,
                    "modified_ts": row_in.modified_ts
                })
                return

            # If we have switched to a new bug
            if self.prevBugID < self.currBugID:
                if self.prevBugID>0:
                    # Start replaying versions in ascending order to build full data on each version
                    if DEBUG_STATUS: Log.note("Emitting intermediate versions for {{bug_id}}", {"bug_id":self.prevBugID})
                    self.populateIntermediateVersionObjects()
                if row_in.bug_id == STOP_BUG:
                    return
                self.startNewBug(row_in)

            # Bugzilla bug workaround - some values were truncated, introducing uncertainty / errors:
            # https://bugzilla.mozilla.org/show_bug.cgi?id=55161
            if row_in.field_name in TRUNC_FIELDS:
                added=CNV.value2string(row_in.new_value)
                removed=CNV.value2string(row_in.old_value)
                uncertain = False


                if added in ["? ?", "?"]: # Unknown value extracted from a possibly truncated field
                    uncertain = True
                    Log.note("PROBLEM Encountered uncertain added value.  Skipping.")
                    row_in.new_value = Null
                elif added != Null and added.startswith("? "): # Possibly truncated value extracted from a possibly truncated field
                    uncertain = True
                    row_in.new_value = added[2:]

                if removed in ["? ?", "?"]:# Unknown value extracted from a possibly truncated field
                    uncertain = True
                    Log.note("PROBLEM Encountered uncertain removed value.  Skipping.")
                    row_in.old_value = Null
                elif removed != Null and removed.startswith("? "): # Possibly truncated value extracted from a possibly truncated field
                    uncertain = True
                    row_in.old_value = removed[2:]

                if uncertain and self.currBugState.uncertain == Null:
                    # Process the "uncertain" flag as an activity
                    # WE ARE GOING BACKWARDS IN TIME, SO MARKUP PAST
                    Log.note("PROBLEM Setting this bug to be uncertain.")
                    self.processBugsActivitiesTableItem(struct.wrap({
                        "modified_ts": row_in.modified_ts,
                        "modified_by": row_in.modified_by,
                        "field_name":"uncertain",
                        "new_value":Null,
                        "old_value":"1",
                        "attach_id":Null
                    }))
                    if row_in.new_value == Null and row_in.old_value == Null:
                        Log.note("Nothing added or removed. Skipping update.")
                        return

            # Treat timestamps as int values
            new_value = CNV.value2int(row_in.new_value) if row_in.field_name.endswith("_ts") else row_in.new_value
            if row_in.field_name=="bug_file_loc" and (row_in.new_value == Null or len(row_in.new_value)>0):
                if DEBUG_STATUS: Log.note("bug_file_loc is empty")
            # Determine where we are in the bug processing workflow
            if row_in._merge_order==1:
                self.processSingleValueTableItem(row_in.field_name, new_value)
            elif row_in._merge_order==2:
                self.processMultiValueTableItem(row_in.field_name, new_value)
            elif row_in._merge_order==7:
                self.processAttachmentsTableItem(row_in)
            elif row_in._merge_order==8:
                self.processFlagsTableItem(row_in)
            elif row_in._merge_order==9:
                self.processBugsActivitiesTableItem(row_in)
            else:
                Log.warning("Unhandled merge_order: '" + row_in._merge_order + "'")

        except Exception, e:
            Log.warning("Problem processing row: {{row}}", {"row":row_in}, e)
        finally:
            if row_in._merge_order>1 and self.currBugState.created_ts == Null:
                Log.note("PROBLEM expecting a created_ts (did you install the timezone database into your MySQL instance?)")

            for b in self.currBugState.blocked:
                if isinstance(b, basestring):
                    Log.note("PROBLEM error")
            self.prev_row=row_in

    @staticmethod
    def uid(bug_id, modified_ts):
        if modified_ts == Null:
            Log.error("modified_ts can not be Null")

        return unicode(bug_id) + "_" + unicode(modified_ts)[0:-3]

    def startNewBug(self, row_in):
        self.prevBugID = row_in.bug_id
        self.bugVersions = []
        self.bugVersionsMap = Struct()
        self.currActivity = Struct()
        self.currBugAttachmentsMap = Struct()
        self.currBugState = Struct(
            _id=parse_bug_history_.uid(row_in.bug_id, row_in.modified_ts),
            bug_id=row_in.bug_id,
            modified_ts=row_in.modified_ts,
            modified_by=row_in.modified_by,
            reported_by=row_in.modified_by,
            attachments=[]
        )
        #WE FORCE ADD ALL SETS, AND WE WILL scrub() THEM OUT LATER IF NOT USED
        for f in MULTI_FIELDS:
            self.currBugState[f]=set([])
        self.currBugState.flags=[]   #FLAGS ARE MULTI_FIELDS, BUT ARE ALSO STRUCTS, SO MUST BE IN AN ARRAY


        if row_in._merge_order != 1:
            # Problem: No entry found in the 'bugs' table.
            Log.warning("Current bugs table record not found for bug_id: {{bug_id}}  (merge order should have been 1, but was {{start_time}})", row_in)


    def processSingleValueTableItem(self, field_name, new_value):
        self.currBugState[field_name] = new_value

    def processMultiValueTableItem(self, field_name, new_value):
        if field_name in NUMERIC_FIELDS: new_value=int(new_value)
        try:
            self.currBugState[field_name].add(new_value)
            return Null
        except Exception, e:
            Log.warning("Unable to push {{value}} to array field {{start_time}} on bug {{curr_value}} current value: {{curr_value}}",{
                "value":new_value,
                "field":field_name,
                "bug_id":self.currBugID,
                "curr_value":self.currBugState[field_name]
            }, e)


    def processAttachmentsTableItem(self, row_in):
        if row_in.attach_id==349397:
            Log.debug("")

        currActivityID = parse_bug_history_.uid(self.currBugID, row_in.modified_ts)
        if currActivityID != self.prevActivityID:
            self.prevActivityID = currActivityID

            self.currActivity =Struct(
                _id=currActivityID,
                modified_ts=row_in.modified_ts,
                modified_by= row_in.modified_by,
                changes= [{
                    "field_name":"attachment_added",
                    "attach_id":row_in.attach_id
                }]
            )

            self.bugVersions.append(self.currActivity)
            self.bugVersionsMap[currActivityID] = self.currActivity


        att=self.currBugAttachmentsMap[unicode(row_in.attach_id)]
        if att == Null:
            att={
                "attach_id": row_in.attach_id,
                "modified_ts": row_in.modified_ts,
                "created_ts": row_in.created_ts,
                "modified_by": row_in.modified_by,
                "flags": []
            }
            self.currBugAttachmentsMap[unicode(row_in.attach_id)]=att

        att["created_ts"]=Math.min([row_in.modified_ts, att["created_ts"]])
        if row_in.field_name=="created_ts" and row_in.new_value == Null:
            pass
        else:
            att[row_in.field_name] = row_in.new_value

            
    def processFlagsTableItem(self, row_in):
        flag = self.makeFlag(row_in.new_value, row_in.modified_ts, row_in.modified_by)
        if row_in.attach_id != Null:
            if self.currBugAttachmentsMap[unicode(row_in.attach_id)] == Null:
                Log.note("Unable to find attachment {{attach_id}} for bug_id {{bug_id}}", {
                    "attach_id":row_in.attach_id,
                    "bug_id":self.currBugID
                })

            self.currBugAttachmentsMap[unicode(row_in.attach_id)].flags.append(flag)
        else:
            self.currBugState.flags.append(flag)


    def processBugsActivitiesTableItem(self, row_in):
        if self.currBugState.created_ts == Null:
            Log.error("must have created_ts")

        if row_in.field_name == "flagtypes_name":
            row_in.field_name = "flags"

        multi_field_new_value = self.getMultiFieldValue(row_in.field_name, row_in.new_value)
        multi_field_old_value = self.getMultiFieldValue(row_in.field_name, row_in.old_value)

        currActivityID = parse_bug_history_.uid(self.currBugID, row_in.modified_ts)
        if currActivityID != self.prevActivityID:
            self.currActivity = self.bugVersionsMap[currActivityID]
            if self.currActivity == Null:
                self.currActivity = Struct(
                    _id= currActivityID,
                    modified_ts= row_in.modified_ts,
                    modified_by= row_in.modified_by,
                    changes= []
                )
                self.bugVersions.append(self.currActivity)

            self.prevActivityID = currActivityID


        if row_in.attach_id != Null:
            attachment = self.currBugAttachmentsMap[unicode(row_in.attach_id)]
            if attachment == Null:
                #we are going backwards in time, no need to worry about these?  maybe delete this change for public bugs
                Log.note("PROBLEM Unable to find attachment {{attach_id}} for bug_id {{start_time}}: {{start_time}}", {
                    "attach_id":row_in.attach_id,
                    "bug_id":self.currBugID,
                    "attachments":self.currBugAttachmentsMap
                })
                self.currActivity.changes.append({
                    "field_name": row_in.field_name,
                    "new_value": row_in.new_value,
                    "old_value": row_in.old_value,
                    "attach_id": row_in.attach_id
                })
            else:

                if row_in.field_name in MULTI_FIELDS:
                    total = attachment[row_in.field_name]
                    # Can have both added and removed values.
                    total=self.removeValues(total, multi_field_new_value, "added", row_in.field_name, "attachment", attachment, row_in.modified_ts)
                    total=self.addValues(total, multi_field_old_value, "removed attachment", row_in.field_name, attachment)
                    attachment[row_in.field_name]=total
                else:
                    attachment[row_in.field_name] = row_in.old_value
                    self.currActivity.changes.append({
                        "field_name": row_in.field_name,
                        "new_value": row_in.new_value,
                        "old_value": row_in.old_value,
                        "attach_id": row_in.attach_id
                    })

        else:
            if row_in.field_name in MULTI_FIELDS:
                # PROBLEM: WHEN GOING BACK IN HISTORY, AND THE ADDED VALUE IS NOT FOUND IN THE CURRENT
                # STATE, IT IS STILL RECORDED (see above self.currActivity.changes.append...).  THIS MEANS
                # WHEN GOING THROUGH THE CHANGES IN IN ORDER THE VALUE WILL EXIST, BUT IT SHOULD NOT
                total = self.currBugState[row_in.field_name]
                # Can have both added and removed values.
                total = self.removeValues(total, multi_field_new_value, "added", row_in.field_name, "currBugState", self.currBugState, row_in.modified_ts)
                total = self.addValues(total, multi_field_old_value, "removed bug", row_in.field_name, self.currBugState)
                self.currBugState[row_in.field_name]=total
            else:
                # Replace current value
                self.currBugState[row_in.field_name] = row_in.old_value
                self.currActivity.changes.append({
                    "field_name": row_in.field_name,
                    "new_value": row_in.new_value,
                    "old_value": row_in.old_value,
                    "attach_id": row_in.attach_id
                })

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
        # Make sure the self.bugVersions are in descending order by modification time.
        # They could be mixed because of attachment activity
        self.bugVersions=Q.sort(self.bugVersions, [
                {"field":"modified_ts", "sort":-1}
        ])

        # Tracks the previous distinct value for field
        prevValues ={}
        currVersion=Null
        # Prime the while loop with an empty next version so our first iteration outputs the initial bug state
        nextVersion = Struct(_id=self.currBugState._id, changes=[])

        flagMap ={}
        # A monotonically increasing version number (useful for debugging)
        self.bug_version_num = 1

        # continue if there are more bug versions, or there is one final nextVersion
        while len(self.bugVersions) > 0 or nextVersion != Null:
            try:
                currVersion = nextVersion
                if len(self.bugVersions) > 0:
                    nextVersion = self.bugVersions.pop() # Oldest version
                else:
                    nextVersion = Null

                # if nextVersion.modified_ts==933875761000:
                #     Log.println("")

                if DEBUG_STATUS: Log.note("Populating JSON for version {{id}}", {"id":currVersion._id})
                # Decide whether to merge this bug activity into the current state (without emitting
                # a separate JSON document). This addresses the case where an attachment is created
                # at exactly the same time as the bug itself.
                # Effectively, we combine all the changes for a given timestamp into the last one.
                mergeBugVersion = False
                if nextVersion != Null and currVersion._id == nextVersion._id:
                    if DEBUG_STATUS: Log.note("Merge mode: activated " + self.currBugState._id)
                    mergeBugVersion = True

                # Link this version to the next one (if there is a next one)
                if nextVersion != Null:
                    if DEBUG_STATUS: Log.note("We have a nextVersion: {{timestamp}} (ver {{next_version}})", {
                        "timestamp":nextVersion.modified_ts,
                        "next_version":self.bug_version_num + 1
                    })
                    self.currBugState.expires_on = nextVersion.modified_ts
                else:
                    # Otherwise, we don't know when the version expires.
                    if DEBUG_STATUS: Log.note("Last bug_version_num = {{version}}", {"version": self.bug_version_num})

                    self.currBugState.expires_on = MAX_TIME

                # Copy all attributes from the current version into self.currBugState
                for propName, propValue in currVersion.items():
                    self.currBugState[propName] = propValue

                # Now walk self.currBugState forward in time by applying the changes from currVersion
                #BE SURE TO APPLY REMOVES BEFORE ADDS, JUST IN CASE BOTH HAPPENED TO ONE FIELD
                changes = Q.sort(currVersion.changes, ["attach_id", "field_name", {"field":"old_value", "sort":-1}, "new_value"])
                currVersion.changes = changes
                self.currBugState.changes = changes

                for c, change in enumerate(changes):
                    if c + 1 < len(changes):
                        #PACK ADDS AND REMOVES TO SINGLE CHANGE TO MATCH ORIGINAL
                        next = changes[c + 1]
                        if change.attach_id == next.attach_id and\
                            change.field_name == next.field_name and\
                            change.old_value != Null and\
                            next.old_value == Null:
                            next.old_value = change.old_value
                            changes[c] = Null
                            continue
                        if change.new_value == Null and \
                            change.old_value == Null and \
                            change.field_name!="attachment_added":
                            changes[c] = Null
                            continue

                    if DEBUG_CHANGES: ("Processing change: " + CNV.object2JSON(change))
                    target = self.currBugState
                    targetName = "currBugState"
                    attach_id = change.attach_id
                    if attach_id != Null:

                        # Handle the special change record that signals the creation of the attachment
                        if change.field_name == "attachment_added":
                            # This change only exists when the attachment has been added to the map, so no missing case needed.
                            att=self.currBugAttachmentsMap[unicode(attach_id)]
                            self.currBugState.attachments.append(att)
                            continue
                        else:
                            # Attachment change
                            target = self.currBugAttachmentsMap[unicode(attach_id)]
                            # target.tada="test"+unicode(currVersion.modified_ts)
                            targetName = "attachment"
                            if target == Null:
                                Log.warning("Encountered a change to missing attachment for bug {{version}}: {{change}}", {
                                    "version": currVersion["bug_id"],
                                    "change": change
                                })

                                # treat it as a change to the main bug instead :(
                                target = self.currBugState
                                targetName = "currBugState"

                    if change.field_name == "flags":
                        self.processFlagChange(target, change, currVersion.modified_ts, currVersion.modified_by)
                    elif change.field_name in MULTI_FIELDS:
                        a = target[change.field_name]
                        multi_field_value = parse_bug_history_.getMultiFieldValue(change.field_name, change.new_value)
                        multi_field_value_removed = parse_bug_history_.getMultiFieldValue(change.field_name, change.old_value)

                        # This was a deletion, find and delete the value(s)
                        a = self.removeValues(a, multi_field_value_removed, "removed", change.field_name, targetName, target, currVersion.modified_ts)
                        # Handle addition(s) (if any)
                        a = self.addValues(a, multi_field_value, "added", change.field_name, target)
                        target[change.field_name]=a
                    else:
                        # Simple field change.
                        # Track the previous value
                        # Single-value field has changed in bug or attachment
                        # Make sure it's actually changing.  We seem to get change
                        # entries for attachments that show the current field value.
                        if target[change.field_name] != change.new_value:
                            self.setPrevious(target, change.field_name, target[change.field_name], currVersion.modified_ts)

                        target[change.field_name] = change.new_value


                self.currBugState.bug_version_num = self.bug_version_num

                if not mergeBugVersion:
                    # This is not a "merge", so output a row for this bug version.
                    self.bug_version_num+=1
                    # Output this version if either it was modified after start_time, or if it
                    # expired after start_time (the latter will update the last known version of the bug
                    # that did not have a value for "expires_on").
                    if self.currBugState.modified_ts >= self.settings.start_time or self.currBugState.expires_on >= self.settings.start_time:
                        state=normalize(self.currBugState)
                        if state.blocked != Null and len(state.blocked)==1 and "Null" in state.blocked:
                            Log.note("PROBLEM error")
                        if DEBUG_STATUS: Log.note("Bug {{bug_state.bug_id}} v{{bug_state.bug_version_num}} (id = {{bug_state.id}})" , {
                            "bug_state":state
                        })
                        self.output.add({"id": state.id, "value": state})  #ES EXPECTED FORMAT

                    else:
                        Log.note("PROBLEM Not outputting {{_id}} - it is before self.start_time ({{start_time}})", {
                            "_id":self.currBugState._id,
                            "start_time":self.settings.start_time
                        })

                else:
                    Log.note("Merging a change with the same timestamp = {{bug_state._id}}: {{bug_state}}",{
                        "bug_state":currVersion
                    })
            finally:
                if self.currBugState.blocked == Null:
                    Log.note("expecting a created_ts")
                pass
            
    def findFlag(self, flag_list, flag):
        for f in flag_list:
            if f.value==flag.value:
                return f

            if (
                f.request_type == flag.request_type and
                f.request_status == flag.request_status and
                self.alias(f.requestee)==self.alias(flag.requestee)
            ):
                Log.note("Using bzAliases to match change '" + flag.value + "' to '" + f.value + "'")
                return f
        return Null


    def processFlagChange(self, target, change, modified_ts, modified_by, reverse=False):
        if target.flags == Null:
            Log.note("PROBLEM  processFlagChange called with unset 'flags'")
            target.flags = []

        addedFlags = parse_bug_history_.getMultiFieldValue("flags", change.new_value)
        removedFlags = parse_bug_history_.getMultiFieldValue("flags", change.old_value)

        #going in reverse when traveling through bugs backwards in time
        if reverse:
            (addedFlags, removedFlags)=(removedFlags, addedFlags)

        # First, mark any removed flags as straight-up deletions.
        for flagStr in removedFlags:
            if flagStr == "":
                continue

            removed_flag = parse_bug_history_.makeFlag(flagStr, modified_ts, modified_by)
            existingFlag = self.findFlag(target.flags, removed_flag)

            if existingFlag != Null:
                # Carry forward some previous values:
                existingFlag["previous_modified_ts"] = existingFlag["modified_ts"]
                existingFlag["modified_ts"] = modified_ts
                if existingFlag["modified_by"] != modified_by:
                    existingFlag["previous_modified_by"] = existingFlag["modified_by"]
                    existingFlag["modified_by"] = modified_by

                # Add changed stuff:
                existingFlag["previous_status"] = removed_flag["request_status"]
                existingFlag["request_status"] = "d"
                existingFlag["previous_value"] = flagStr
                existingFlag["value"] = Null            #SPECIAL INDICATOR
                # request_type stays the same.
                # requestee stays the same.

                duration_ms = existingFlag["modified_ts"] - existingFlag["previous_modified_ts"]
                existingFlag["duration_days"] = math.floor(duration_ms / (1000.0 * 60 * 60 * 24))
            else:
                Log.warning("Did not find a corresponding flag for removed value {{removed}} in {{existing}}",{
                    "removed":flagStr,
                    "existing":target.flags
                })

        # See if we can align any of the added flags with previous deletions.
        # If so, try to match them up with a "dangling" removed flag
        for flagStr in addedFlags:
            if flagStr == "":
                continue

            added_flag = self.makeFlag(flagStr, modified_ts, modified_by)

            candidates = [element for element in target.flags if
                element["value"] == Null    #SPECIAL INDICATOR
                    and added_flag["request_type"] == element["request_type"]
                    and added_flag["request_status"] != element["previous_status"] # Skip "r?(dre@mozilla)" -> "r?(mark@mozilla)"
            ]

            if len(candidates)==0:
                # No matching candidate. Totally new flag.
                target.flags.append(added_flag)
                continue

            chosen_one = candidates[0]
            if len(candidates) > 1:
                # Multiple matches - use the best one.
                Log.note("Matched added flag {{flag}} to multiple removed flags {{candidates}}.  Using the best.", {
                    "flag":added_flag,
                    "candidates":candidates
                })
                matched_ts = [element for element in candidates if
                    added_flag.modified_ts == element.modified_ts
                ]

                if len(matched_ts) == 1:
                    Log.note("Matching on modified_ts fixed it")
                    chosen_one = matched_ts[0]
                else:
                    Log.note("Matching on modified_ts left us with {{num}} matches", {"num":len(matched_ts)})
                    # If we had no matches (or many matches), try matching on requestee.
                    matched_req = [element for element in candidates if
                        # Do case-insenitive comparison
                        element["requestee"] != Null and
                            added_flag["modified_by"].lower() == element["requestee"].lower()
                    ]
                    if len(matched_req) == 1:
                        Log.note("Matching on requestee fixed it")
                        chosen_one = matched_req[0]
                    else:
                        Log.warning("Matching on requestee left us with {{num}} matches. Skipping match.", {"num":len(matched_req)})
                        # TODO: add "uncertain" flag?
                        chosen_one = Null
            else:
                # Obvious case - matched exactly one.
                Log.note("Matched added flag " + CNV.object2JSON(added_flag) + " to removed flag " + CNV.object2JSON(chosen_one))

            if chosen_one != Null:
                for f in ["value", "request_status", "requestee"]:
                    chosen_one[f] = nvl(added_flag[f], chosen_one[f])

            # We need to avoid later adding this flag twice, since we rolled an add into a delete.




    def setPrevious(self, dest, aFieldName, aValue, aChangeAway):
        if dest["previous_values"] == Null:
            dest["previous_values"] ={}

        pv = dest["previous_values"]
        vField = aFieldName + "_value"
        caField = aFieldName + "_change_away_ts"
        ctField = aFieldName + "_change_to_ts"
        ddField = aFieldName + "_duration_days"

        pv[vField] = aValue
        # If we have a previous change for this field, then use the
        # change-away time as the new change-to time.
        if pv[caField] != Null:
            pv[ctField] = pv[caField]
        else:
            # Otherwise, this is the first change for this field, so
            # use the creation timestamp.
            pv[ctField] = dest["created_ts"]

        pv[caField] = aChangeAway
        try:
            duration_ms = pv[caField] - pv[ctField]
        except Exception, e:
            Log.error("", e)
        pv[ddField] = math.floor(duration_ms / (1000.0 * 60 * 60 * 24))




    @staticmethod
    def makeFlag(flag, modified_ts, modified_by):
        flagParts = Struct(
            modified_ts=modified_ts,
            modified_by=modified_by,
            value=flag
        )

        matches = FLAG_PATTERN.match(flag)
        if matches:
            flagParts.request_type = matches.group(1)
            flagParts.request_status = matches.group(2)
            if matches.start(3)!=-1 and len(matches.group(3)) > 2:
                flagParts.requestee = matches.group(3)[1:-1]


        return flagParts


    def addValues(self, total, add, valueType, field_name, target):
        if len(add)==0: return total
#        Log.note("Adding " + valueType + " " + fieldName + " values:" + CNV.object2JSON(someValues))
        if field_name == "flags":
            for v in add:
                total.append(parse_bug_history_.makeFlag(v, target.modified_ts, target.modified_by))
            if valueType!="added":
                self.currActivity.changes.append({
                    "field_name": field_name,
                    "new_value": Null,
                    "old_value": ", ".join(Q.sort(add)),
                    "attach_id": target.attach_id
                })
            return total
            ## TODO: Some bugs (like 685605) actually have duplicate flags. Do we want to keep them?
            #/*
            # # Check if this flag has already been incorporated into a removed flag. If so, don't add it again.
            # dupes = anArray.filter(def(element, index, array):
            # return element["value"] == added
            # and element["modified_by"] == anObj.modified_by
            # and element["modified_ts"] == anObj.modified_ts
            # })
            # if dupes and dupes.length > 0:
            # Log.note("Skipping duplicated added flag '" + added + "' since info is already in " + CNV.object2JSON(dupes[0]))
            # else:
            # */
        else:
            diff=add-total
            removed=total&add

            #WE CAN NOT REMOVE VALUES WE KNOW TO BE THERE AFTER
            if len(removed)>0:
                Log.note("PROBLEM: Found {{type}}({{bug_id}}).{{field_name}} value: (Removing {{removed}} can not result in {{existing}})",{
                    "bug_id":target.bug_id,
                    "type":valueType,
                    "field_name":field_name,
                    "removed":removed,
                    "existing":target[field_name]
                })

            if valueType!="added" and len(diff)>0:
                self.currActivity.changes.append({
                    "field_name": field_name,
                    "new_value": Null,
                    "old_value": ", ".join(map(unicode, Q.sort(diff))),
                    "attach_id": target.attach_id
                })

            return total | add




    def removeValues(self, total, remove, valueType, field_name, arrayDesc, target, timestamp):
        if field_name == "flags":
            removeMe=[]
            for v in remove:
                flag = parse_bug_history_.makeFlag(v, 0, 0)

                found=self.findFlag(total, flag)
                if found != Null:
                    removeMe.append(found.value) #FOR SOME REASON, REMOVAL BY OBJECT DOES NOT WORK
                else:
                    Log.note("PROBLEM Unable to find {{type}} FLAG: {{object}}.{{field_name}}: (All {{missing}}" + " not in : {{existing}})",{
                        "type":valueType,
                        "object":arrayDesc,
                        "field_name":field_name,
                        "missing":v,
                        "existing":target[field_name]
                    })

            total=[a for a in total if a.value not in removeMe]

            if valueType=="added" and len(removeMe)>0:
                try:
                    self.currActivity.changes.append({
                        "field_name": field_name,
                        "new_value": u", ".join(Q.sort(removeMe)),
                        "old_value": Null,
                        "attach_id": target.attach_id
                    })
                except Exception, e:
                    Log.error("problem", e)
            return total
        elif field_name == "keywords":
            diff = remove - total
            output = total - remove

            if valueType=="added":
                self.currActivity.changes.append({
                    "field_name": field_name,
                    "new_value": u", ".join(map(unicode, Q.sort(remove))),
                    "old_value": Null,
                    "attach_id": target.attach_id
                })

            if len(diff)>0:
                Log.note("PROBLEM Unable to find {{type}} KEYWORD {{object}}({{bug_id}}) (adding anyway): (All {{missing}}" + " not in : {{existing}})",{
                    "bug_id":target.bug_id,
                    "type":valueType,
                    "object":arrayDesc,
                    "field_name":field_name,
                    "missing":diff,
                    "existing":target[field_name]
                })
            return output
        elif field_name == "cc":
            # MAP CANONICAL TO EXISTING (BETWEEN map_* AND self.aliases WE HAVE A BIJECTION)
            map_total={self.alias(t):t for t in total}
            map_remove={self.alias(r):r for r in remove}
            # CANONICAL VALUES
            c_total=set(map_total.keys())
            c_remove=set(map_remove.keys())

            removed = c_total & c_remove
            diff = c_remove - c_total
            output = c_total - c_remove

            if not target.uncertain:
                for lost in diff:
                    details = self.aliases[lost.replace(".", "\.")]
                    if details == Null:
                        details = Struct()
                        self.aliases[lost.replace(".", "\.")] = details
                    details.last_seen = Math.max([details.last_seen, timestamp])

                    if not details.candidates:
                        details.candidates = Multiset(output)
                    else:
                        details.candidates.extend(output)

                    found = clear_winner(details.candidates)
                    if found:
                        Log.note("ALIAS FOUND: {{lost}} == {{found}}", {
                            "lost":lost,
                            "found":found
                        })

                        self.add_alias(lost, found, timestamp)
                        new_lost=self.alias(lost)  # IN CASE THE ALIAS CHANGED
                        removed.add(new_lost)
                        map_total[lost]=map_total[found]
                        if new_lost!=found:
                            Log.error("not expected")
                        output.discard(found)
                    else:
                        def shorten(emails):
                            if len(emails)>5:
                                return"["+str(len(target[field_name]))+" email addresses]"
                            else:
                                return CNV.object2JSON(emails)

                        Log.note("PROBLEM: Unable to find CC: {{missing}} not in : {{existing}} (candidates={{candidates}})",{
                            "type":valueType,
                            "object":arrayDesc,
                            "field_name":field_name,
                            "missing":lost,
                            "existing":shorten(target[field_name]),
                            "candidates":shorten(details.candidates.keys())
                        })
            else:
                # PATTERN MATCH EMAIL ADDRESSES
                for lost in diff:
                    best_score = 0.3
                    best = Null
                    for found in output:
                        score = Math.min([
                            strings.edit_distance(found, lost),
                            strings.edit_distance(found.split("@")[0], lost.split("@")[0]),
                            strings.edit_distance(map_total[found], lost),
                            strings.edit_distance(map_total[found].split("@")[0], lost.split("@")[0])
                        ])
                        if score<best_score:
                            best_score=score
                            best=found
                    if best!=Null:
                        Log.note("UNCERTAIN ALIAS FOUND: {{lost}} == {{found}}", {
                            "lost":lost,
                            "found":best
                        })
                        #DO NOT SAVE THE ALIAS, IT MAY BE WRONG
                        removed.add(best)
                        output.discard(best)
                    else:
                        Log.note("PROBLEM Unable to pattern match {{type}} value: {{object}}.{{field_name}}: ({{missing}}" + " not in : {{existing}})",{
                            "type":valueType,
                            "object":arrayDesc,
                            "field_name":field_name,
                            "missing":lost,
                            "existing":target[field_name]
                        })

            if valueType=="added":
                # DURING WALK BACK IN TIME, WE POPULATE THE changes
                try:
                    if removed - set(map_total.keys()):
                        Log.error("problem with alias finding:\n"+
                                  "map_total={{map_total}}\n"+
                                  "map_remove={{map_remove}}\n"+
                                  "c_total={{c_total}}\n"+
                                  "c_remove={{c_remove}}\n"+
                                  "removed={{removed}}\n"+
                                  "diff={{diff}}\n"+
                                  "output={{output}}\n", {
                                  "map_total":map_total,
                                  "c_total":c_total,
                                  "map_remove":map_remove,
                                  "c_remove":c_remove,
                                  "removed":removed,
                                  "diff":diff,
                                  "output":output
                        })
                    final_removed = [map_total[r] for r in removed]
                    self.currActivity.changes.append({
                        "field_name": field_name,
                        "new_value": u", ".join(map(unicode, Q.sort(final_removed))),
                        "old_value": Null,
                        "attach_id": target.attach_id
                    })
                except Exception, e:
                    Log.error("issues", e)

            return {map_total[o] for o in output}
        else:
            removed = total & remove
            diff = remove - total
            output = total - remove

            if valueType=="added":
                self.currActivity.changes.append({
                    "field_name": field_name,
                    "new_value": u", ".join(map(unicode, Q.sort(removed))),
                    "old_value": Null,
                    "attach_id": target.attach_id
                })

            if len(diff)>0:
                Log.note("PROBLEM Unable to find {{type}} value in {{bug_id}}: {{object}}.{{field_name}}: (All {{missing}}" + " not in : {{existing}})",{
                    "bug_id":target.bug_id,
                    "type":valueType,
                    "object":arrayDesc,
                    "field_name":field_name,
                    "missing":diff,
                    "existing":target[field_name]
                })

            return output



    @staticmethod
    def getMultiFieldValue(name, value):
        if value == Null:
            return set()
        if name in MULTI_FIELDS:
            if name in NUMERIC_FIELDS:
                return set([int(s.strip()) for s in value.split(",") if s.strip()!=""])
            else:
                return set([s.strip() for s in value.split(",") if s.strip()!=""])

        return {value}


    def alias(self, name):
        if name == Null:
            return Null
        alias=self.aliases[name.replace(".", "\.")].canonical

        if alias == Null and name.endswith("@formerly-netscape.com.tld"):
            canonical = name.replace("@formerly-netscape.com.tld", "@netscape.com")
            self.add_alias(name, canonical, 0)
            return canonical

        return nvl(alias, name)

    def add_alias(self, lost, found, timestamp):
        found_record = self.aliases[found.replace(".", "\.")]
        if found_record != Null:
            new_canonical = nvl(found_record.canonical, found)
            found_record.last_seen = Math.max([found_record.last_seen, timestamp])
        else:
            new_canonical = found
            found_record = {"last_seen": timestamp, "canonical": found}
            self.aliases[found.replace(".", "\.")] = found_record

        lost_record = self.aliases[lost.replace(".", "\.")]
        if lost_record != Null:
            old_canonical = nvl(lost_record.canonical, lost)
            lost_record.canonical = new_canonical
            lost_record.last_seen = Math.max([lost_record.last_seen, timestamp])
            lost_record.candidates = Null
        else:
            old_canonical = lost
            lost_record = {"last_seen": timestamp, "canonical": new_canonical}
            self.aliases[lost.replace(".", "\.")] = lost_record

        if old_canonical != new_canonical:
            for k, v in self.aliases.items():
                if v.canonical == old_canonical:
                    Log.note("ALIAS REMAPPED: {{alias}}->{{old}} to {{alias}}->{{new}}",{
                        "alias":k,
                        "old":old_canonical,
                        "new":new_canonical
                    })
                    v.canonical = new_canonical



    def initializeAliases(self):
        try:
            try:
                alias_json = File(self.settings.alias_file).read()
            except Exception, e:
                alias_json = "{}"
            self.aliases = CNV.JSON2object(alias_json)

            Log.note("{{num}} aliases loaded", {"num": len(self.aliases.keys())})

            for v in self.aliases.values():
                v.candidates=CNV.dict2Multiset(v.candidates)
        except Exception, e:
            Log.error("Can not init aliases", e)

    def saveAliases(self):
        for k, v in self.aliases.items():
            v.candidates=CNV.multiset2dict(v.candidates)

        Log.note("{{num}} aliases saved", {"num": len(self.aliases.keys())})

        alias_json = CNV.object2JSON(self.aliases, pretty=True)
        file = File(self.settings.alias_file)
        # file = File(file.backup_name())
        file.write(alias_json)



CLEARLY = 2

def clear_winner(candidates):
    """
    RETURN THE ELEMENT THAT HAS CLEARLY MORE HITS THAN THE OTHERS
    """
    if candidates == Null or not candidates.keys():
        return Null

    if not isinstance(candidates, Multiset):
        Log.error("Expecting multiset")

    ordered=Q.sort([{"k":k, "c":c} for k, c in candidates.dic.items()], "c")
    best=ordered[-1]

    # SOME EMAIL ADDRESSES MATCH TOO WELL
    clearly = CLEARLY
    # if best["k"] in ("reidr@pobox.com"):
    #     clearly += 2

    if len(ordered) == 1 and best["c"] >= clearly:
        return ordered[-1]["k"]
    elif len(ordered) > 1 and best["c"] >= ordered[-2]["c"] + clearly:
        return ordered[-1]["k"]
    else:
        return Null