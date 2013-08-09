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
from sphinx.builders import changes
from util.cnv import CNV
from util.debug import D
from util.map import Map

FLAG_PATTERN = re.compile("^(.*)([?+-])(\\([^)]*\\))?$")

# Used to reformat incoming dates into the expected form.
# Example match: "2012/01/01 00:00:00.000"
DATE_PATTERN_STRICT = re.compile("^[0-9]{4}[\\/-][0-9]{2}[\\/-][0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}")
# Example match: "2012-08-08 0:00"
DATE_PATTERN_RELAXED = re.compile("^[0-9]{4}[\\/-][0-9]{2}[\\/-][0-9]{2}")

# Fields that could have been truncated per bug 55161
TRUNC_FIELDS = ["cc", "blocked", "dependson", "keywords"]

def setup(param, output_queue):
    bzAliases =None
    currBugID =None
    prevBugID =None
    bugVersions =None
    bugVersionsMap =None
    currBugState =None
    currBugAttachmentsMap =None
    prevActivityID=None
    currActivity =None
    START_TIME = int(param.START_TIME)
    END_TIME = int(param.END_TIME)
    output=output_queue


    def processRow(**row_in):
        row_in=Map(**row_in)
        # param.bug_id, param.modified_ts, param.modified_by, param.field_name, param.field_value_in, param.field_value_removed, param.attach_id, param._merge_order):
        currBugID = row_in.bug_id

        if not bzAliases:
            initializeAliases()

        D.println(CNV.object2JSON(row_in))

        # For debugging purposes:
        if END_TIME > 0 and row_in.modified_ts > END_TIME:
            D.println("Skipping change after END_TIME (" + END_TIME + ")")
            return

        # If we have switched to a new bug
        if prevBugID < currBugID:
            # Start replaying versions in ascending order to build full data on each version
            D.println("Emitting intermediate versions for " + prevBugID)
            populateIntermediateVersionObjects()
            startNewBug(row_in.bug_id, row_in.modified_ts, row_in.modified_by, row_in._merge_order)

        # Bugzilla bug workaround - some values were truncated, introducing uncertainty / errors:
        # https://bugzilla.mozilla.org/show_bug.cgi?id=55161
        if row_in.field_name in TRUNC_FIELDS:
            uncertain = False

            # Unknown value extracted from a possibly truncated field
            if row_in.field_value_in == "? ?" or row_in.field_value_removed == "? ?":
                uncertain = True
                if row_in.field_value_in == "? ?":
                    D.println("Encountered uncertain added value.  Skipping.")
                    param.field_value_in = ""

                if row_in.field_value_removed == "? ?":
                    D.println("Encountered uncertain removed value.  Skipping.")
                    param.field_value_removed = ""


            # Possibly truncated value extracted from a possibly truncated field
            if row_in.field_value_in.startswith("? "):
                uncertain = True
                param.field_value_in = row_in.field_value_in[2:]

            if row_in.field_value_removed.startswith("? "):
                uncertain = True
                param.field_value_removed = row_in.field_value_removed[2:]

            if uncertain:
                # Process the "uncertain" flag as an activity
                D.println("Setting this bug to be uncertain.")
                processBugsActivitiesTableItem(row_in.modified_ts, row_in.modified_by, "uncertain", "1", "", "")
                if row_in.field_value_in == "" and row_in.field_value_removed == "":
                    D.println("Nothing added or removed. Skipping update.")
                    return



        # Treat timestamps as int values
        field_value = int(row_in.field_value_in) if row_in.field_name.endswith("_ts") else row_in.field_value_in

        if currBugID < 999999999:
            # Determine where we are in the bug processing workflow
            if row_in._merge_order==1:
                processSingleValueTableItem(row_in.field_name, field_value)
            elif row_in._merge_order==2:
                processMultiValueTableItem(row_in.field_name, field_value)
            elif row_in._merge_order==7:
                processAttachmentsTableItem(row_in.modified_ts, row_in.modified_by, row_in.field_name, field_value, row_in.field_value_removed, row_in.attach_id)
            elif row_in._merge_order==8:
                processFlagsTableItem(row_in.modified_ts, row_in.modified_by, row_in.field_name, field_value, row_in.field_value_removed, row_in.attach_id)
            elif row_in._merge_order==9:
                processBugsActivitiesTableItem(row_in.modified_ts, row_in.modified_by, row_in.field_name, field_value, row_in.field_value_removed, row_in.attach_id)
            else:
                D.warning("Unhandled merge_order: '" + row_in._merge_order + "'")



    def startNewBug(bug_id, modified_ts, modified_by, merge_order):
        if currBugID >= 999999999: return

        prevBugID = bug_id
        bugVersions = []
        bugVersionsMap ={}
        currActivity ={}
        currBugAttachmentsMap ={}
        currBugState = {
            bug_id: bug_id,
            modified_ts: modified_ts,
            modified_by: modified_by,
            reported_by: modified_by,
            attachments: [],
            flags: []
        }
        currBugState._id = bug_id + "." + modified_ts

        if merge_order != 1:
            # Problem: No entry found in the 'bugs' table.
            D.warning("Current bugs table record not found for bug_id: "
                + bug_id + " (merge order should have been 1, but was " + merge_order + ")")


    def processSingleValueTableItem(field_name, field_value):
        currBugState[field_name] = field_value

    def processMultiValueTableItem(field_name, field_value):
        if currBugState[field_name] is None:
            currBugState[field_name] = []

        try:
            currBugState[field_name].push(field_value)
        except Exception, e:
            D.warning("Unable to push " + field_value + " to array field "
                + field_name + " on bug " + currBugID + " current value:"
                + CNV.object2JSON(currBugState[field_name]))


    def processAttachmentsTableItem(modified_ts, modified_by, field_name, field_value, field_value_removed, attach_id):
        currActivityID = currBugID + "." + modified_ts
        if currActivityID != prevActivityID:
            currActivity ={
                _id: currActivityID,
                modified_ts: modified_ts,
                modified_by: modified_by,
                changes: []
            }

            bugVersions.push(currActivity)
            bugVersionsMap[currActivityID] = currActivity
            prevActivityID = currActivityID
            currActivity.changes.push({
                field_name: "attachment_added",
                attach_id: attach_id
            })

        if not currBugAttachmentsMap[attach_id]:
            currBugAttachmentsMap[attach_id] ={
                attach_id: attach_id,
                modified_ts: modified_ts,
                modified_by: modified_by,
                flags: []
            }

        currBugAttachmentsMap[attach_id][field_name] = field_value

    def processFlagsTableItem(modified_ts, modified_by, field_name, field_value, field_value_removed, attach_id):
        flag = makeFlag(field_value, modified_ts, modified_by)
        if attach_id != '':
            if not currBugAttachmentsMap[attach_id]:
                D.warning("Unable to find attachment " + attach_id + " for bug_id " + currBugID)

            currBugAttachmentsMap[attach_id].flags.push(flag)
        else:
            currBugState.flags.push(flag)


    def processBugsActivitiesTableItem(modified_ts, modified_by, field_name, field_value, field_value_removed, attach_id):
        if field_name == "flagtypes.name":
            field_name = "flags"

        multi_field_value = getMultiFieldValue(field_name, field_value)
        multi_field_value_removed = getMultiFieldValue(field_name, field_value_removed)

        currActivityID = currBugID + "." + modified_ts
        if currActivityID != prevActivityID:
            currActivity = bugVersionsMap[currActivityID]
            if not currActivity:
                currActivity ={
                    _id: currActivityID,
                    modified_ts: modified_ts,
                    modified_by: modified_by,
                    changes: []
                }
                bugVersions.push(currActivity)

            prevActivityID = currActivityID

        currActivity.changes.push({
            field_name: field_name,
            field_value: field_value,
            field_value_removed: field_value_removed,
            attach_id: attach_id
        })
        if attach_id != '':
            attachment = currBugAttachmentsMap[attach_id]
            if not attachment:
                D.warning("Unable to find attachment " + attach_id + " for bug_id "
                    + currBugID + ": " + CNV.object2JSON(currBugAttachmentsMap))
            else:
                if isinstance(attachment[field_name], list):
                    a = attachment[field_name]
                    # Can have both added and removed values.
                    if multi_field_value[0] != '':
                        removeValues(a, multi_field_value, "added", field_name, "attachment", attachment)

                    if multi_field_value_removed[0] != '':
                        addValues(a, multi_field_value_removed, "removed attachment", field_name, currActivity)

                else:
                    attachment[field_name] = field_value_removed


        else:
            if isinstance(currBugState[field_name], list):
                a = currBugState[field_name]
                # Can have both added and removed values.
                if multi_field_value[0] != '':
                    removeValues(a, multi_field_value, "added", field_name, "currBugState", currBugState)

                if multi_field_value_removed[0] != '':
                    addValues(a, multi_field_value_removed, "removed bug", field_name, currActivity)

            elif isMultiField(field_name):
                # field must currently be missing, otherwise it would
                # be an instanceof Array above.  This handles multi-valued
                # fields that are not first processed by processMultiValueTableItem().
                currBugState[field_name] = multi_field_value_removed
            else:
                # Replace current value
                currBugState[field_name] = field_value_removed



    def sortAscByField(a, b, aField):
        if a[aField] > b[aField]:
            return 1
        if a[aField] < b[aField]:
            return -1
        return 0

    def sortDescByField(a, b, aField):
        return -1 * sortAscByField(a, b, aField)

    def populateIntermediateVersionObjects():
        # Make sure the bugVersions are in descending order by modification time.
        # They could be mixed because of attachment activity
        bugVersions.sort(lambda(a, b):
            sortDescByField(a, b, "modified_ts")
        )

        # Tracks the previous distinct value for field
        prevValues ={}
        currVersion=None
        # Prime the while loop with an empty next version so our first iteration outputs the initial bug state
        nextVersion ={
            "_id":currBugState._id,
            "changes":[]
        }
        flagMap ={}
        # A monotonically increasing version number (useful for debugging)
        currBugVersion = 1

        # continue if there are more bug versions, or there is one final nextVersion
        while bugVersions.length > 0 or nextVersion:
            currVersion = nextVersion
            if bugVersions.length > 0:
                nextVersion = bugVersions.pop() # Oldest version
            else:
                nextVersion = None

            D.println("Populating JSON for version " + currVersion._id)

            # Decide whether to merge this bug activity into the current state (without emitting
            # a separate JSON document). This addresses the case where an attachment is created
            # at exactly the same time as the bug itself.
            # Effectively, we combine all the changes for a given timestamp into the last one.
            mergeBugVersion = False
            if nextVersion and currVersion._id == nextVersion._id:
                D.println("Merge mode: activated " + currBugState._id)
                mergeBugVersion = True

            # Link this version to the next one (if there is a next one)
            if nextVersion:
                D.println("We have a nextVersion:" + nextVersion.modified_ts
                    + " (ver " + (currBugVersion + 1) + ")")
                currBugState.expires_on = nextVersion.modified_ts
            else:
                # Otherwise, we don't know when the version expires.
                D.println("We have no nextVersion after #" + currBugVersion)
                currBugState.expires_on = None

            # Copy all attributes from the current version into currBugState
            for propName in currVersion:
                currBugState[propName] = currVersion[propName]

            # Now walk currBugState forward in time by applying the changes from currVersion
            changes = currVersion.changes
            #D.println("Processing changes: "+CNV.object2JSON(changes))
            for change in changes:
                D.println("Processing change: " + CNV.object2JSON(change))
                target = currBugState
                targetName = "currBugState"
                attachID = change["attach_id"]
                if attachID != '':
                    # Handle the special change record that signals the creation of the attachment
                    if change.field_name == "attachment_added":
                        # This change only exists when the attachment has been added to the map, so no missing case needed.
                        currBugState.attachments.push(currBugAttachmentsMap[attachID])
                        continue
                    else:
                        # Attachment change
                        target = currBugAttachmentsMap[attachID]
                        targetName = "attachment"
                        if target is None:
                            D.warning("Encountered a change to missing attachment for bug '"
                                + currVersion["bug_id"] + "': " + CNV.object2JSON(change) + ".")

                            # treat it as a change to the main bug instead :(
                            target = currBugState
                            targetName = "currBugState"



                # Track the previous value
                if not isMultiField(change.field_name):
                    # Single-value field has changed in bug or attachment
                    # Make sure it's actually changing.  We seem to get change
                    #  entries for attachments that show the current field value.
                    if target[change.field_name] != change.field_value:
                        setPrevious(target, change.field_name, target[change.field_name], currVersion.modified_ts)
                    else:
                        D.println("Skipping fake change to " + targetName + ": "
                            + CNV.object2JSON(target) + ", change: " + CNV.object2JSON(change))

                elif change.field_name == "flags":
                    processFlagChange(target, change, currVersion.modified_ts, currVersion.modified_by)
                else:
                    D.println("Skipping previous_value for " + targetName
                        + " multi-value field " + change.field_name)

                # Multi-value fields
                if change.field_name == "flags":
                    # Already handled by "processFlagChange" above.
                    D.println("Skipping previously processed flag change")
                elif isinstance(target[change.field_name], list):
                    a = target[change.field_name]
                    multi_field_value = getMultiFieldValue(change.field_name, change.field_value)
                    multi_field_value_removed = getMultiFieldValue(change.field_name, change.field_value_removed)

                    # This was a deletion, find and delete the value(s)
                    if multi_field_value_removed[0] != '':
                        removeValues(a, multi_field_value_removed, "removed", change.field_name, targetName, target)

                    # Handle addition(s) (if any)
                    addValues(a, multi_field_value, "added", change.field_name, currVersion)
                elif isMultiField(change.field_name):
                    # First appearance of a multi-value field
                    target[change.field_name] = [change.field_value]
                else:
                    # Simple field change.
                    target[change.field_name] = change.field_value


            # Do some processing to make sure that diffing betweens runs stays as similar as possible.
            stabilize(currBugState)

            # Empty string breaks ES date parsing, remove it from bug state.
            for dateField in ["deadline", "cf_due_date", "cf_last_resolved"]:
                # Special case to handle values that can't be parsed by ES:
                if currBugState[dateField] == "":
                    # Skip empty strings
                    currBugState[dateField] = None


            # Also reformat some date fields
            for dateField in ["deadline", "cf_due_date"]:
                if currBugState[dateField] and currBugState[dateField].match(DATE_PATTERN_RELAXED):
                    # Convert "2012/01/01 00:00:00.000" to "2012-01-01"
                    # Example: bug 643420 (deadline)
                    #          bug 726635 (cf_due_date)
                    currBugState[dateField] = currBugState[dateField].substring(0, 10).replace("/", '-')


            for dateField in ["cf_last_resolved"]:
                if currBugState[dateField] and currBugState[dateField].match(DATE_PATTERN_STRICT):
                    # Convert "2012/01/01 00:00:00.000" to "2012-01-01T00:00:00.000Z", then to a timestamp.
                    # Example: bug 856732 (cf_last_resolved)
                    # dateString = currBugState[dateField].substring(0, 10).replace("/", '-') + "T" + currBugState[dateField].substring(11) + "Z"
                    currBugState[dateField] = CNV.datetime2unixmilli(CNV.string2datetime(currBugState[dateField]+"000", "%Y/%m/%d %H%M%S%f"))


            currBugState.bug_version_num = currBugVersion

            if not mergeBugVersion:
                # This is not a "merge", so output a row for this bug version.
                currBugVersion+=1
                # Output this version if either it was modified after START_TIME, or if it
                # expired after START_TIME (the latter will update the last known version of the bug
                # that did not have a value for "expires_on").
                if currBugState.modified_ts >= START_TIME or currBugState.expires_on >= START_TIME:
                    # Emit this version as a JSON string
                    #bugJSON = CNV.object2JSON(currBugState,None,2); # DEBUGGING, expanded output
                    bugJSON = CNV.object2JSON(currBugState) #condensed output

                    D.println("Bug " + currBugState.bug_id + " v" + currBugState.bug_version_num + " (_id = " + currBugState._id + "): " + bugJSON)
                    newRow = {
                        "bug_id":currBugState.bug_id,
                        "_id":currBugState._id,
                        "json":bugJSON
                    }

                    output.add(newRow)
                else:
                    D.println("Not outputting " + currBugState._id
                        + " - it is before START_TIME (" + START_TIME + ")")

            else:
                D.println("Merging a change with the same timestamp = " + currBugState._id + ": " + CNV.object2JSON(currVersion))



    def findFlag(aFlagList, aFlag):
        existingFlag = findByKey(aFlagList, "value", aFlag.value)
        if not existingFlag:
            for eFlag in aFlagList:
                if (eFlag.request_type == aFlag.request_type
                    and eFlag.request_status == aFlag.request_status
                    and (bzAliases[aFlag.requestee + "=" + eFlag.requestee] # Try both directions.
                    or bzAliases[eFlag.requestee + "=" + aFlag.requestee])):
                    D.println("Using bzAliases to match change '" + aFlag.value + "' to '" + eFlag.value + "'")
                    existingFlag = eFlag
                    break



        return existingFlag

    def processFlagChange(aTarget, aChange, aTimestamp, aModifiedBy):
        addedFlags = getMultiFieldValue("flags", aChange.field_value)
        removedFlags = getMultiFieldValue("flags", aChange.field_value_removed)

        # First, mark any removed flags as straight-up deletions.
        for flagStr in removedFlags:
            if flagStr == "":
                continue

            flag = makeFlag(flagStr, aTimestamp, aModifiedBy)
            existingFlag = findFlag(aTarget["flags"], flag)

            if existingFlag:
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
                existingFlag["duration_days"] = Math.floor(duration_ms / (1000.0 * 60 * 60 * 24))
            else:
                D.warning("Did not find a corresponding flag for removed value '"
                    + flagStr + "' in " + CNV.object2JSON(aTarget["flags"]))


        # See if we can align any of the added flags with previous deletions.
        # If so, try to match them up with a "dangling" removed flag
        for flagStr in addedFlags:
            if flagStr == "":
                continue

            flag = makeFlag(flagStr, aTimestamp, aModifiedBy)

            if not aTarget["flags"]:
                D.println("Warning: processFlagChange called with unset 'flags'")
                aTarget["flags"] = []

            candidates = aTarget["flags"].filter(lambda(element, index, array):
                element["value"] == ""
                    and flag["request_type"] == element["request_type"]
                    and flag["request_status"] != element["previous_status"] # Skip "r?(dre@mozilla)" -> "r?(mark@mozilla)"
            )

            if candidates:
                if candidates.length >= 1:
                    chosen_one = candidates[0]
                    if candidates.length > 1:
                        # Multiple matches - use the best one.
                        D.println("Matched added flag " + CNV.object2JSON(flag) + " to multiple removed flags.  Using the best of these:")
                        for candidate in candidates:
                            D.println("      " + CNV.object2JSON(candidate))

                        matched_ts = candidates.filter(lambda(element, index, array):
                            flag["modified_ts"] == element["modified_ts"]
                        )
                        if matched_ts and matched_ts.length == 1:
                            D.println("Matching on modified_ts fixed it")
                            chosen_one = matched_ts[0]
                        else:
                            D.println("Matching on modified_ts left us with " + (matched_ts.length  if matched_ts  else  "no")+ " matches")
                            # If we had no matches (or many matches), try matching on requestee.
                            matched_req = candidates.filter(lambda(element, index, array):
                                # Do case-insenitive comparison
                                flag["modified_by"].toLowerCase() == element["requestee"].toLowerCase() if element["requestee"] else False
                            )
                            if matched_req and matched_req.length == 1:
                                D.println("Matching on requestee fixed it")
                                chosen_one = matched_req[0]
                            else:
                                D.warning("Matching on requestee left us with " +( matched_req.length if matched_req else "no")+ " matches. Skipping match.")
                                # TODO: add "uncertain" flag?
                                chosen_one = None


                    else:
                        # Obvious case - matched exactly one.
                        D.println("Matched added flag " + CNV.object2JSON(flag) + " to removed flag " + CNV.object2JSON(chosen_one))

                    if chosen_one:
                        for f in ["value", "request_status", "requestee"]:
                            if flag[f]:
                                chosen_one[f] = flag[f]



                    # We need to avoid later adding this flag twice, since we rolled an add into a delete.
                else:
                    # No matching candidate. Totally new flag.
                    D.println("Did not match added flag " + CNV.object2JSON(flag) + " to anything: " + CNV.object2JSON(aTarget["flags"]))
                    aTarget["flags"].push(flag)




    def setPrevious(dest, aFieldName, aValue, aChangeAway):
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
        if pv[caField]:
            pv[ctField] = pv[caField]
        else:
            # Otherwise, this is the first change for this field, so
            # use the creation timestamp.
            pv[ctField] = dest["created_ts"]

        pv[caField] = aChangeAway
        duration_ms = pv[caField] - pv[ctField]
        pv[ddField] = Math.floor(duration_ms / (1000.0 * 60 * 60 * 24))

    def findByKey(aList, aField, aValue):
        for item in aList:
            if item[aField] == aValue:
                return item


        return None

    def stabilize(aBug):
        if aBug["cc"] and aBug["cc"][0]:
            aBug["cc"].sort()

        if aBug["changes"]:
            aBug["changes"].sort(lambda(a, b):
                sortAscByField(a, b, "field_name")
            )


    def makeFlag(flag, modified_ts, modified_by):
        flagParts ={
            "modified_ts": modified_ts,
            "modified_by": modified_by,
            "value": flag
        }

        matches = FLAG_PATTERN.match(flag)
        if matches:
            flagParts.request_type = matches[1]
            flagParts.request_status = matches[2]
            if matches[3] and matches[3].length > 2:
                flagParts.requestee = matches[3].substring(1, matches[3].length - 1)


        return flagParts

    def addValues(anArray, someValues, valueType, fieldName, anObj):
        D.println("Adding " + valueType + " " + fieldName + " values:" + CNV.object2JSON(someValues))
        if fieldName == "flags":
            for added in someValues:
                if added != '':
                    # TODO: Some bugs (like 685605) actually have duplicate flags. Do we want to keep them?
    #                /*
    #                 # Check if this flag has already been incorporated into a removed flag. If so, don't add it again.
    #                 dupes = anArray.filter(def(element, index, array):
    #                 return element["value"] == added
    #                 and element["modified_by"] == anObj.modified_by
    #                 and element["modified_ts"] == anObj.modified_ts
    #                 })
    #                 if dupes and dupes.length > 0:
    #                 D.println("Skipping duplicated added flag '" + added + "' since info is already in " + CNV.object2JSON(dupes[0]))
    #                 else:
    #                 */
                    addedFlag = makeFlag(added, anObj.modified_ts, anObj.modified_by)
                    anArray.push(addedFlag)


        else:
            for added in someValues:
                if added != '':
                    anArray.push(added)




    def removeValues(anArray, someValues, valueType, fieldName, arrayDesc, anObj):
        if fieldName == "flags":
            for v in someValues:
                len = anArray.length
                flag = makeFlag(v, 0, 0)
                for i in range(len(anArray)):
                    # Match on complete flag (incl. status) and flag value
                    if anArray[i].value == v:
                        anArray.splice(i, 1)
                        break
                    elif flag.requestee:
                        # Match on flag type and status, then use Aliases to match
                        # TODO: Should we try exact matches all the way to the end first?
                        if (anArray[i].request_type == flag.request_type
                            and anArray[i].request_status == flag.request_status
                            and bzAliases[flag.requestee + "=" + anArray[i].requestee]):
                            D.println("Using bzAliases to match '" + v + "' to '" + anArray[i].value + "'")
                            anArray.splice(i, 1)
                            break



                if len == anArray.length:
                    D.warning("Unable to find " + valueType + " flag " + fieldName + ":" + v
                        + " in " + arrayDesc + ": " + CNV.object2JSON(anObj))


        else:
            for v in someValues:
                foundAt = anArray.indexOf(v)
                if foundAt >= 0:
                    anArray.splice(foundAt, 1)
                else:
                    logLevel = "e"
                    if fieldName == "cc":
                        # Don't make too much noise about mismatched cc items.
                        logLevel = "d"

                    writeToLog(logLevel, "Unable to find " + valueType + " value " + fieldName + ":" + v
                        + " in " + arrayDesc + ": " + CNV.object2JSON(anObj))




    def isMultiField(aFieldName):
        return (aFieldName == "flags" or aFieldName == "cc" or aFieldName == "keywords"
            or aFieldName == "dependson" or aFieldName == "blocked" or aFieldName == "dupe_by"
            or aFieldName == "dupe_of" or aFieldName == "bug_group" or aFieldName == "see_also")

    def getMultiFieldValue(aFieldName, aFieldValue):
        if isMultiField(aFieldName):
            return [s.strip() for s in aFieldValue.split(",")]

        return [aFieldValue]

    def initializeAliases():
        BZ_ALIASES = getVariable("BZ_ALIASES", 0)
        bzAliases ={}
        if BZ_ALIASES:
            D.println("Initializing aliases")
            for alias in [s.strip() for s in BZ_ALIASES.split(",")]:
                D.println("Adding alias '" + alias + "'")
                bzAliases[alias] = True

        else:
            D.println("Not initializing aliases")


    return processRow




