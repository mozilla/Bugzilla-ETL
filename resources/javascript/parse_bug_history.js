/* vim: set filetype=javascript ts=2 et sw=2: */
/* Workflow:
 Create the current state object

 For each row containing latest state data (fields from bugs table record, fields from other tables (i.e. attachments, dependencies)
 Update the current state object with the latest field values

 Walk backward through activity records from bugs_activity (and other activity type tables). For each set of activities:
 Create a new bug version object with the meta data about this activity
 Set id based on modification time
 *       Set valid_from field as modification time
 *       Set valid_to field as the modification time of the later version - 1 second
 Add modification data (who, when, what)
 For single value fields (i.e. assigned_to, status):
 Update the original state object by replacing the field value with the contents of the activities "removed" column
 For multi-value fields (i.e. blocks, CC, attachments):
 If a deletion, update the original state object by adding the value from the "removed" column to the field values array.
 If an addition, find and remove the added item from the original state object

 When finished with all activities, the current state object should reflect the original state of the bug when created.
 Now, build the full state of each intermediate version of the bug.

 For each bug version object that was created above:
 Merge the current state object into this version object
 Update fields according to the modification data

 When doing an incremental update (ie. with start_time specified), Look at any bug that has been modified since the
 cutoff time, and build all versions.  Only index versions after start_time in ElasticSearch.

 */

// Used to split a flag into (type, status [,requestee])
// Example: "review?(mreid@mozilla.com)" -> (review, ?, mreid@mozilla.com)
// Example: "review-" -> (review, -)
const FLAG_PATTERN = /^(.*)([?+-])(\([^)]*\))?$/;

// Used to reformat incoming dates into the expected form.
// Example match: "2012/01/01 00:00:00.000"
const DATE_PATTERN_STRICT = /^[0-9]{4}[\/-][0-9]{2}[\/-][0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}.[0-9]{3}/;
// Example match: "2012-08-08 0:00"
const DATE_PATTERN_RELAXED = /^[0-9]{4}[\/-][0-9]{2}[\/-][0-9]{2} /;

// Fields that could have been truncated per bug 55161
const TRUNC_FIELDS = ["cc", "blocked", "dependson", "keywords"];

var bzAliases = null;
var currBugID;
var prevBugID;
var bugVersions;
var bugVersionsMap;
var currBugState;
var currBugAttachmentsMap;
var prevActivityID;
var currActivity;
var inputRowSize = getInputRowMeta().size();
var outputRowSize = getOutputRowMeta().size();
var start_time = parseInt(getVariable("start_time", 0));
var end_time = parseInt(getVariable("end_time", 0));

function processRow(bug_id, modified_ts, modified_by, field_name, field_value_in, field_value_removed, attach_id, _merge_order) {
    currBugID = bug_id;

    if (!bzAliases) {
        initializeAliases();
    }

    writeToLog("d", "bug_id={" + bug_id + "}, modified_ts={" + modified_ts + "}, modified_by={" + modified_by
        + "}, field_name={" + field_name + "}, field_value={" + field_value_in + "}, field_value_removed={"
        + field_value_removed + "}, attach_id={" + attach_id + "}, _merge_order={" + _merge_order + "}");

    // For debugging purposes:
    if (end_time > 0 && modified_ts > end_time) {
        writeToLog("l", "Skipping change after end_time (" + end_time + ")");
        return;
    }

    // If we have switched to a new bug
    if (prevBugID < currBugID) {
        // Start replaying versions in ascending order to build full data on each version
        writeToLog("d", "Emitting intermediate versions for " + prevBugID);
        populateIntermediateVersionObjects();
        startNewBug(bug_id, modified_ts, modified_by, _merge_order);
    }

    // Bugzilla bug workaround - some values were truncated, introducing uncertainty / errors:
    // https://bugzilla.mozilla.org/show_bug.cgi?id=55161
    if (TRUNC_FIELDS.indexOf(field_name) >= 0) {
        var uncertain = false;

        // Unknown value extracted from a possibly truncated field
        if (field_value_in == "? ?" || field_value_removed == "? ?") {
            uncertain = true;
            if (field_value_in == "? ?") {
                writeToLog("d", "Encountered uncertain added value.  Skipping.");
                field_value_in = "";
            }
            if (field_value_removed == "? ?") {
                writeToLog("d", "Encountered uncertain removed value.  Skipping.");
                field_value_removed = "";
            }
        }

        // Possibly truncated value extracted from a possibly truncated field
        if (field_value_in.indexOf("? ") == 0) {
            uncertain = true;
            field_value_in = field_value_in.substring(2);
        }
        if (field_value_removed.indexOf("? ") == 0) {
            uncertain = true;
            field_value_removed = field_value_removed.substring(2);
        }

        if (uncertain) {
            // Process the "uncertain" flag as an activity
            writeToLog("d", "Setting this bug to be uncertain.");
            processBugsActivitiesTableItem(modified_ts, modified_by, "uncertain", "1", "", "");
            if (field_value_in == "" && field_value_removed == "") {
                writeToLog("d", "Nothing added or removed. Skipping update.");
                return;
            }
        }
    }

    // Treat timestamps as int values
    var field_value = field_name.match(/_ts$/) ? parseInt(field_value_in) : field_value_in;

    if (currBugID < 999999999) {
        // Determine where we are in the bug processing workflow
        switch (_merge_order) {
            case 1:
                processSingleValueTableItem(field_name, field_value);
                break;
            case 2:
                processMultiValueTableItem(field_name, field_value);
                break;
            case 7:
                processAttachmentsTableItem(modified_ts, modified_by, field_name, field_value, field_value_removed, attach_id);
                break;
            case 8:
                processFlagsTableItem(modified_ts, modified_by, field_name, field_value, field_value_removed, attach_id);
                break;
            case 9:
                processBugsActivitiesTableItem(modified_ts, modified_by, field_name, field_value, field_value_removed, attach_id);
                break;
            default:
                writeToLog("e", "Unhandled merge_order: '" + _merge_order + "'");
                break;
        }
    }
}

function startNewBug(bug_id, modified_ts, modified_by, merge_order) {
    if (currBugID >= 999999999) return;

    prevBugID = bug_id;
    bugVersions = [];
    bugVersionsMap = {};
    currActivity = {};
    currBugAttachmentsMap = {};
    currBugState = {
        bug_id: bug_id,
        modified_ts: modified_ts,
        modified_by: modified_by,
        reported_by: modified_by,
        attachments: [],
        flags: []
    };
    currBugState._id = bug_id + "." + modified_ts;

    if (merge_order != 1) {
        // Problem: No entry found in the 'bugs' table.
        writeToLog("e", "Current bugs table record not found for bug_id: "
            + bug_id + " (merge order should have been 1, but was " + merge_order + ")");
    }
}

function processSingleValueTableItem(field_name, field_value) {
    currBugState[field_name] = field_value;
}

function processMultiValueTableItem(field_name, field_value) {
    if (currBugState[field_name] == null) {
        currBugState[field_name] = [];
    }
    try {
        currBugState[field_name].push(field_value);
    } catch(e) {
        writeToLog("e", "Unable to push " + field_value + " to array field "
            + field_name + " on bug " + currBugID + " current value:"
            + JSON.stringify(currBugState[field_name]));
    }
}

function processAttachmentsTableItem(modified_ts, modified_by, field_name, field_value, field_value_removed, attach_id) {
    currActivityID = currBugID + "." + modified_ts;
    if (currActivityID != prevActivityID) {
        currActivity = {
            _id: currActivityID,
            modified_ts: modified_ts,
            modified_by: modified_by,
            changes: []
        };
        bugVersions.push(currActivity);
        bugVersionsMap[currActivityID] = currActivity;
        prevActivityID = currActivityID;
        currActivity.changes.push({
            field_name: "attachment_added",
            attach_id: attach_id
        });
    }
    if (!currBugAttachmentsMap[attach_id]) {
        currBugAttachmentsMap[attach_id] = {
            attach_id: attach_id,
            modified_ts: modified_ts,
            modified_by: modified_by,
            flags: []
        };
    }
    currBugAttachmentsMap[attach_id][field_name] = field_value;
}

function processFlagsTableItem(modified_ts, modified_by, field_name, field_value, field_value_removed, attach_id) {
    var flag = makeFlag(field_value, modified_ts, modified_by);
    if (attach_id != '') {
        if (!currBugAttachmentsMap[attach_id]) {
            writeToLog("e", "Unable to find attachment " + attach_id + " for bug_id " + currBugID);
        }
        currBugAttachmentsMap[attach_id].flags.push(flag);
    } else {
        currBugState.flags.push(flag);
    }
}

function processBugsActivitiesTableItem(modified_ts, modified_by, field_name, field_value, field_value_removed, attach_id) {
    if (field_name == "flagtypes.name") {
        field_name = "flags";
    }

    var multi_field_value = getMultiFieldValue(field_name, field_value);
    var multi_field_value_removed = getMultiFieldValue(field_name, field_value_removed);

    currActivityID = currBugID + "." + modified_ts;
    if (currActivityID != prevActivityID) {
        currActivity = bugVersionsMap[currActivityID];
        if (!currActivity) {
            currActivity = {
                _id: currActivityID,
                modified_ts: modified_ts,
                modified_by: modified_by,
                changes: []
            };
            bugVersions.push(currActivity);
        }
        prevActivityID = currActivityID;
    }
    currActivity.changes.push({
        field_name: field_name,
        field_value: field_value,
        field_value_removed: field_value_removed,
        attach_id: attach_id
    });
    if (attach_id != '') {
        var attachment = currBugAttachmentsMap[attach_id];
        if (!attachment) {
            writeToLog("e", "Unable to find attachment " + attach_id + " for bug_id "
                + currBugID + ": " + JSON.stringify(currBugAttachmentsMap));
        } else {
            if (attachment[field_name] instanceof Array) {
                var a = attachment[field_name];
                // Can have both added and removed values.
                if (multi_field_value[0] != '') {
                    removeValues(a, multi_field_value, "added", field_name, "attachment", attachment);
                }

                if (multi_field_value_removed[0] != '') {
                    addValues(a, multi_field_value_removed, "removed attachment", field_name, currActivity);
                }
            } else {
                attachment[field_name] = field_value_removed;
            }
        }
    } else {
        if (currBugState[field_name] instanceof Array) {
            var a = currBugState[field_name];
            // Can have both added and removed values.
            if (multi_field_value[0] != '') {
                removeValues(a, multi_field_value, "added", field_name, "currBugState", currBugState);
            }

            if (multi_field_value_removed[0] != '') {
                addValues(a, multi_field_value_removed, "removed bug", field_name, currActivity);
            }
        } else if (isMultiField(field_name)) {
            // field must currently be missing, otherwise it would
            // be an instanceof Array above.  This handles multi-valued
            // fields that are not first processed by processMultiValueTableItem().
            currBugState[field_name] = multi_field_value_removed;
        } else {
            // Replace current value
            currBugState[field_name] = field_value_removed;
        }
    }
}

function sortAscByField(a, b, aField) {
    if (a[aField] > b[aField])
        return 1;
    if (a[aField] < b[aField])
        return -1;
    return 0;
}

function sortDescByField(a, b, aField) {
    return -1 * sortAscByField(a, b, aField);
}

function populateIntermediateVersionObjects() {
    // Make sure the bugVersions are in descending order by modification time.
    // They could be mixed because of attachment activity
    bugVersions.sort(function(a, b) {
        return sortDescByField(a, b, "modified_ts")
    });

    // Tracks the previous distinct value for each field
    var prevValues = {};

    var currVersion;
    // Prime the while loop with an empty next version so our first iteration outputs the initial bug state
    var nextVersion = {_id:currBugState._id,changes:[]};

    var flagMap = {};

    // A monotonically increasing version number (useful for debugging)
    var currBugVersion = 1;

    // continue if there are more bug versions, or there is one final nextVersion
    while (bugVersions.length > 0 || nextVersion) {
        currVersion = nextVersion;
        if (bugVersions.length > 0) {
            nextVersion = bugVersions.pop(); // Oldest version
        } else {
            nextVersion = undefined;
        }
        writeToLog("d", "Populating JSON for version " + currVersion._id);

        // Decide whether to merge this bug activity into the current state (without emitting
        // a separate JSON document). This addresses the case where an attachment is created
        // at exactly the same time as the bug itself.
        // Effectively, we combine all the changes for a given timestamp into the last one.
        var mergeBugVersion = false;
        if (nextVersion && currVersion._id == nextVersion._id) {
            writeToLog("d", "Merge mode: activated " + currBugState._id);
            mergeBugVersion = true;
        }

        // Link this version to the next one (if there is a next one)
        if (nextVersion) {
            writeToLog("d", "We have a nextVersion:" + nextVersion.modified_ts
                + " (ver " + (currBugVersion + 1) + ")");
            currBugState.expires_on = nextVersion.modified_ts;
        } else {
            // Otherwise, we don't know when the version expires.
            writeToLog("d", "We have no nextVersion after #" + currBugVersion);
            currBugState.expires_on = null;
        }

        // Copy all attributes from the current version into currBugState
        for (var propName in currVersion) {
            currBugState[propName] = currVersion[propName];
        }

        // Now walk currBugState forward in time by applying the changes from currVersion
        var changes = currVersion.changes;
        //writeToLog("d", "Processing changes: "+JSON.stringify(changes));
        for (var changeIdx = 0; changeIdx < changes.length; changeIdx++) {
            var change = changes[changeIdx];
            writeToLog("d", "Processing change: " + JSON.stringify(change));
            var target = currBugState;
            var targetName = "currBugState";
            var attachID = change["attach_id"];
            if (attachID != '') {
                // Handle the special change record that signals the creation of the attachment
                if (change.field_name == "attachment_added") {
                    // This change only exists when the attachment has been added to the map, so no missing case needed.
                    currBugState.attachments.push(currBugAttachmentsMap[attachID]);
                    continue;
                } else {
                    // Attachment change
                    target = currBugAttachmentsMap[attachID];
                    targetName = "attachment";
                    if (target == null) {
                        writeToLog("e", "Encountered a change to missing attachment for bug '"
                            + currVersion["bug_id"] + "': " + JSON.stringify(change) + ".");

                        // treat it as a change to the main bug instead :(
                        target = currBugState;
                        targetName = "currBugState";
                    }
                }
            }

            // Track the previous value
            if (!isMultiField(change.field_name)) {
                // Single-value field has changed in bug or attachment
                // Make sure it's actually changing.  We seem to get change
                //  entries for attachments that show the current field value.
                if (target[change.field_name] != change.field_value) {
                    setPrevious(target, change.field_name, target[change.field_name], currVersion.modified_ts);
                } else {
                    writeToLog("d", "Skipping fake change to " + targetName + ": "
                        + JSON.stringify(target) + ", change: " + JSON.stringify(change));
                }
            } else if (change.field_name == "flags") {
                processFlagChange(target, change, currVersion.modified_ts, currVersion.modified_by);
            } else {
                writeToLog("d", "Skipping previous_value for " + targetName
                    + " multi-value field " + change.field_name);
            }

            // Multi-value fields
            if (change.field_name == "flags") {
                // Already handled by "processFlagChange" above.
                writeToLog("d", "Skipping previously processed flag change");
            } else if (target[change.field_name] instanceof Array) {
                var a = target[change.field_name];
                var multi_field_value = getMultiFieldValue(change.field_name, change.field_value);
                var multi_field_value_removed = getMultiFieldValue(change.field_name, change.field_value_removed);

                // This was a deletion, find and delete the value(s)
                if (multi_field_value_removed[0] != '') {
                    removeValues(a, multi_field_value_removed, "removed", change.field_name, targetName, target);
                }

                // Handle addition(s) (if any)
                addValues(a, multi_field_value, "added", change.field_name, currVersion);
            } else if (isMultiField(change.field_name)) {
                // First appearance of a multi-value field
                target[change.field_name] = [change.field_value];
            } else {
                // Simple field change.
                target[change.field_name] = change.field_value;
            }
        }

        // Do some processing to make sure that diffing betweens runs stays as similar as possible.
        stabilize(currBugState);

        // Empty string breaks ES date parsing, remove it from bug state.
        for each (var dateField in ["deadline", "cf_due_date", "cf_last_resolved"]) {
            // Special case to handle values that can't be parsed by ES:
            if (currBugState[dateField] == "") {
                // Skip empty strings
                currBugState[dateField] = undefined;
            }
        }

        // Also reformat some date fields
        for each (var dateField in ["deadline", "cf_due_date"]) {
            if (currBugState[dateField] && currBugState[dateField].match(DATE_PATTERN_RELAXED)) {
                // Convert "2012/01/01 00:00:00.000" to "2012-01-01"
                // Example: bug 643420 (deadline)
                //          bug 726635 (cf_due_date)
                currBugState[dateField] = currBugState[dateField].substring(0, 10).replace(/\//g, '-');
            }
        }

        for each (var dateField in ["cf_last_resolved"]) {
            if (currBugState[dateField] && currBugState[dateField].match(DATE_PATTERN_STRICT)) {
                // Convert "2012/01/01 00:00:00.000" to "2012-01-01T00:00:00.000Z", then to a timestamp.
                // Example: bug 856732 (cf_last_resolved)
                var dateString = currBugState[dateField].substring(0, 10).replace(/\//g, '-') + "T" + currBugState[dateField].substring(11) + "Z";
                currBugState[dateField] = "" + new Date(dateString).getTime();
            }
        }

        currBugState.bug_version_num = currBugVersion;

        if (!mergeBugVersion) {
            // This is not a "merge", so output a row for this bug version.
            currBugVersion++;
            // Output this version if either it was modified after start_time, or if it
            // expired after start_time (the latter will update the last known version of the bug
            // that did not have a value for "expires_on").
            if (currBugState.modified_ts >= start_time || currBugState.expires_on >= start_time) {
                // Emit this version as a JSON string
                //var bugJSON = JSON.stringify(currBugState,null,2); // DEBUGGING, expanded output
                var bugJSON = JSON.stringify(currBugState); // condensed output

                writeToLog("d", "Bug " + currBugState.bug_id + " v" + currBugState.bug_version_num + " (_id = " + currBugState._id + "): " + bugJSON);
                var newRow = createRowCopy(outputRowSize);
                var rowIndex = inputRowSize;
                newRow[rowIndex++] = currBugState.bug_id;
                newRow[rowIndex++] = currBugState._id;
                newRow[rowIndex++] = bugJSON;
                putRow(newRow);
            } else {
                writeToLog("d", "Not outputting " + currBugState._id
                    + " - it is before start_time (" + start_time + ")");
            }
        } else {
            writeToLog("d", "Merging a change with the same timestamp = " + currBugState._id + ": " + JSON.stringify(currVersion));
        }

    }
}

function findFlag(aFlagList, aFlag) {
    var existingFlag = findByKey(aFlagList, "value", aFlag.value);
    if (!existingFlag) {
        for each (var eFlag in aFlagList) {
            if (eFlag.request_type == aFlag.request_type
                && eFlag.request_status == aFlag.request_status
                && (bzAliases[aFlag.requestee + "=" + eFlag.requestee] // Try both directions.
                || bzAliases[eFlag.requestee + "=" + aFlag.requestee])) {
                writeToLog("d", "Using bzAliases to match change '" + aFlag.value + "' to '" + eFlag.value + "'");
                existingFlag = eFlag;
                break;
            }
        }
    }
    return existingFlag;
}

function processFlagChange(aTarget, aChange, aTimestamp, aModifiedBy) {
    var addedFlags = getMultiFieldValue("flags", aChange.field_value);
    var removedFlags = getMultiFieldValue("flags", aChange.field_value_removed);

    // First, mark any removed flags as straight-up deletions.
    for each (var flagStr in removedFlags) {
        if (flagStr == "") {
            continue;
        }
        var flag = makeFlag(flagStr, aTimestamp, aModifiedBy);
        var existingFlag = findFlag(aTarget["flags"], flag);

        if (existingFlag) {
            // Carry forward some previous values:
            existingFlag["previous_modified_ts"] = existingFlag["modified_ts"];
            if (existingFlag["modified_by"] != aModifiedBy) {
                existingFlag["previous_modified_by"] = existingFlag["modified_by"];
                existingFlag["modified_by"] = aModifiedBy;
            }

            // Add changed stuff:
            existingFlag["modified_ts"] = aTimestamp;
            existingFlag["previous_status"] = flag["request_status"];
            existingFlag["previous_value"] = flagStr;
            existingFlag["request_status"] = "Log.error";
            existingFlag["value"] = "";
            // request_type stays the same.
            // requestee stays the same.

            var duration_ms = existingFlag["modified_ts"] - existingFlag["previous_modified_ts"];
            existingFlag["duration_days"] = Math.floor(duration_ms / (1000.0 * 60 * 60 * 24));
        } else {
            writeToLog("e", "Did not find a corresponding flag for removed value '"
                + flagStr + "' in " + JSON.stringify(aTarget["flags"]));
        }
    }

    // See if we can align any of the added flags with previous deletions.
    // If so, try to match them up with a "dangling" removed flag
    for each (var flagStr in addedFlags) {
        if (flagStr == "") {
            continue;
        }

        var flag = makeFlag(flagStr, aTimestamp, aModifiedBy);

        if (!aTarget["flags"]) {
            writeToLog("d", "Warning: processFlagChange called with unset 'flags'");
            aTarget["flags"] = [];
        }

        var candidates = aTarget["flags"].filter(function(element, index, array) {
            return (element["value"] == ""
                && flag["request_type"] == element["request_type"]
                && flag["request_status"] != element["previous_status"]); // Skip "r?(dre@mozilla)" -> "r?(mark@mozilla)"
        });

        if (candidates) {
            if (candidates.length >= 1) {
                var chosen_one = candidates[0];
                if (candidates.length > 1) {
                    // Multiple matches - use the best one.
                    writeToLog("d", "Matched added flag " + JSON.stringify(flag) + " to multiple removed flags.  Using the best of these:");
                    for each (var candidate in candidates) {
                        writeToLog("d", "      " + JSON.stringify(candidate));
                    }
                    var matched_ts = candidates.filter(function(element, index, array) {
                        return flag["modified_ts"] == element["modified_ts"];
                    });
                    if (matched_ts && matched_ts.length == 1) {
                        writeToLog("d", "Matching on modified_ts fixed it");
                        chosen_one = matched_ts[0];
                    } else {
                        writeToLog("d", "Matching on modified_ts left us with " + (matched_ts ? matched_ts.length : "no") + " matches");
                        // If we had no matches (or many matches), try matching on requestee.
                        var matched_req = candidates.filter(function(element, index, array) {
                            // Do case-insenitive comparison
                            if (element["requestee"]) {
                                return flag["modified_by"].toLowerCase() == element["requestee"].toLowerCase();
                            }
                            return false;
                        });
                        if (matched_req && matched_req.length == 1) {
                            writeToLog("d", "Matching on requestee fixed it");
                            chosen_one = matched_req[0];
                        } else {
                            writeToLog("e", "Matching on requestee left us with " + (matched_req ? matched_req.length : "no") + " matches. Skipping match.");
                            // TODO: add "uncertain" flag?
                            chosen_one = null;
                        }
                    }
                } else {
                    // Obvious case - matched exactly one.
                    writeToLog("d", "Matched added flag " + JSON.stringify(flag) + " to removed flag " + JSON.stringify(chosen_one));
                }

                if (chosen_one) {
                    for each (var f in ["value", "request_status", "requestee"]) {
                        if (flag[f]) {
                            chosen_one[f] = flag[f];
                        }
                    }
                }
                // We need to avoid later adding this flag twice, since we rolled an add into a delete.
            } else {
                // No matching candidate. Totally new flag.
                writeToLog("d", "Did not match added flag " + JSON.stringify(flag) + " to anything: " + JSON.stringify(aTarget["flags"]));
                aTarget["flags"].push(flag);
            }
        }
    }
}

function setPrevious(dest, aFieldName, aValue, aChangeAway) {
    if (!dest["previous_values"]) {
        dest["previous_values"] = {};
    }

    var pv = dest["previous_values"];
    var vField = aFieldName + "_value";
    var caField = aFieldName + "_change_away_ts";
    var ctField = aFieldName + "_change_to_ts";
    var ddField = aFieldName + "_duration_days";

    pv[vField] = aValue;
    // If we have a previous change for this field, then use the
    // change-away time as the new change-to time.
    if (pv[caField]) {
        pv[ctField] = pv[caField];
    } else {
        // Otherwise, this is the first change for this field, so
        // use the creation timestamp.
        pv[ctField] = dest["created_ts"];
    }
    pv[caField] = aChangeAway;
    var duration_ms = pv[caField] - pv[ctField];
    pv[ddField] = Math.floor(duration_ms / (1000.0 * 60 * 60 * 24));
}

function findByKey(aList, aField, aValue) {
    for each (var item in aList) {
        if (item[aField] == aValue) {
            return item;
        }
    }
    return null;
}

function stabilize(aBug) {
    if (aBug["cc"] && aBug["cc"][0]) {
        aBug["cc"].sort();
    }
    if (aBug["changes"]) {
        aBug["changes"].sort(function(a, b) {
            return sortAscByField(a, b, "field_name")
        });
    }
}

function makeFlag(flag, modified_ts, modified_by) {
    var flagParts = {
        modified_ts: modified_ts,
        modified_by: modified_by,
        value: flag
    };
    var matches = FLAG_PATTERN.exec(flag);
    if (matches) {
        flagParts.request_type = matches[1];
        flagParts.request_status = matches[2];
        if (matches[3] && matches[3].length > 2) {
            flagParts.requestee = matches[3].substring(1, matches[3].length - 1);
        }
    }
    return flagParts;
}

function addValues(anArray, someValues, valueType, fieldName, anObj) {
    writeToLog("d", "Adding " + valueType + " " + fieldName + " values:" + JSON.stringify(someValues));
    if (fieldName == "flags") {
        for each (var added in someValues) {
            if (added != '') {
                // TODO: Some bugs (like 685605) actually have duplicate flags. Do we want to keep them?
                /*
                 // Check if this flag has already been incorporated into a removed flag. If so, don't add it again.
                 var dupes = anArray.filter(function(element, index, array) {
                 return element["value"] == added
                 && element["modified_by"] == anObj.modified_by
                 && element["modified_ts"] == anObj.modified_ts;
                 });
                 if (dupes && dupes.length > 0) {
                 writeToLog("d", "Skipping duplicated added flag '" + added + "' since info is already in " + JSON.stringify(dupes[0]));
                 } else {
                 */
                var addedFlag = makeFlag(added, anObj.modified_ts, anObj.modified_by);
                anArray.push(addedFlag);
                //}
            }
        }
    } else {
        for each (var added in someValues) {
            if (added != '') {
                anArray.push(added);
            }
        }
    }
}

function removeValues(anArray, someValues, valueType, fieldName, arrayDesc, anObj) {
    if (fieldName == "flags") {
        for each (var v in someValues) {
            var len = anArray.length;
            var flag = makeFlag(v, 0, 0);
            for (var i = 0; i < len; i++) {
                // Match on complete flag (incl. status) and flag value
                if (anArray[i].value == v) {
                    anArray.splice(i, 1);
                    break;
                } else if (flag.requestee) {
                    // Match on flag type and status, then use Aliases to match
                    // TODO: Should we try exact matches all the way to the end first?
                    if (anArray[i].request_type == flag.request_type
                        && anArray[i].request_status == flag.request_status
                        && bzAliases[flag.requestee + "=" + anArray[i].requestee]) {
                        writeToLog("d", "Using bzAliases to match '" + v + "' to '" + anArray[i].value + "'");
                        anArray.splice(i, 1);
                        break;
                    }
                }
            }

            if (len == anArray.length) {
                writeToLog("e", "Unable to find " + valueType + " flag " + fieldName + ":" + v
                    + " in " + arrayDesc + ": " + JSON.stringify(anObj));
            }
        }
    } else {
        for each (var v in someValues) {
            var foundAt = anArray.indexOf(v);
            if (foundAt >= 0) {
                anArray.splice(foundAt, 1);
            } else {
                writeToLog("e", "Unable to find " + valueType + " value " + fieldName + ":" + v
                    + " in " + arrayDesc + ": " + JSON.stringify(anObj));
            }
        }
    }
}

function isMultiField(aFieldName) {
    return (aFieldName == "flags" || aFieldName == "cc" || aFieldName == "keywords"
        || aFieldName == "dependson" || aFieldName == "blocked" || aFieldName == "dupe_by"
        || aFieldName == "dupe_of" || aFieldName == "bug_group" || aFieldName == "see_also");
}

function getMultiFieldValue(aFieldName, aFieldValue) {
    if (isMultiField(aFieldName)) {
        return aFieldValue.split(/\s*,\s*/);
    }

    return [aFieldValue];
}

function initializeAliases() {
    var BZ_ALIASES = getVariable("BZ_ALIASES", 0);
    bzAliases = {};
    if (BZ_ALIASES) {
        writeToLog("d", "Initializing aliases");
        for each (var alias in BZ_ALIASES.split(/, */)) {
            writeToLog("d", "Adding alias '" + alias + "'");
            bzAliases[alias] = true;
        }
    } else {
        writeToLog("d", "Not initializing aliases");
    }
}
