# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Author: Kyle Lahnakoski (kyle@lahnakoski.com)
#

# Workflow:
# 1. Create the current state object
#
# 2. for each row containing latest state data (fields from bugs table record, fields from other tables (i.e. attachments, dependencies)
# Update the current state object with the latest field values
#
# 3. Walk backward through activity records from bugs_activity (and other activity type tables). for set of activities:
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


from __future__ import absolute_import
from __future__ import division
from __future__ import unicode_literals

import math
import re

from bugzilla_etl.alias_analysis import AliasAnalyzer
from bugzilla_etl.extract_bugzilla import MAX_TIMESTAMP
from bugzilla_etl.transform_bugzilla import normalize, NUMERIC_FIELDS, MULTI_FIELDS, DIFF_FIELDS, NULL_VALUES, TIME_FIELDS, LONG_FIELDS
from jx_python import jx, meta
from mo_dots import inverse, coalesce, wrap, unwrap, literal_field, listwrap
from mo_dots.datas import Data
from mo_dots.lists import FlatList
from mo_dots.nones import Null
from mo_future import text_type, long, PYPY
from mo_json import value2json
from mo_logs import Log, strings, Except
from mo_logs.strings import apply_diff
from mo_math import MIN, Math
from mo_times import Date
from pyLibrary import convert

# Used to split a flag into (type, status [,requestee])
# Example: "review?(mreid@mozilla.com)" -> (review, ?, mreid@mozilla.com)
# Example: "review-" -> (review, -)
from pyLibrary.convert import value2number

FLAG_PATTERN = re.compile("^(.*)([?+-])(\\([^)]*\\))?$")

DEBUG_CHANGES = True   # SHOW ACTIVITY RECORDS BEING PROCESSED
DEBUG_STATUS = False    # SHOW CURRENT STATE OF PROCESSING
DEBUG_CC_CHANGES = False  # SHOW MISMATCHED CC CHANGES
DEBUG_FLAG_MATCHES = False
DEBUG_MISSING_ATTACHMENTS = False
DEBUG_MEMORY = False
DEBUG_DIFF = False
USE_PREVIOUS_VALUE_OBJECTS = False

# Fields that could have been truncated per bug 55161
TRUNC_FIELDS = ["cc", "blocked", "dependson", "keywords"]
KNOWN_MISSING_KEYWORDS = {
    "dogfood", "beta1", "nsbeta1", "nsbeta2", "nsbeta3", "patch", "mozilla1.0", "correctness",
    "mozilla0.9", "mozilla0.9.9+", "nscatfood", "mozilla0.9.3", "fcc508", "nsbeta1+", "mostfreq"
}
KNOWN_INCONSISTENT_FIELDS = {
    "cf_last_resolved",  # CHANGES IN DATABASE TIMEZONE
    "cf_crash_signature"
}
FIELDS_CHANGED = wrap({
    # SOME FIELD VALUES ARE CHANGED WITHOUT HISTORY BEING CHANGED TOO https://bugzilla.mozilla.org/show_bug.cgi?id=997228
    # MAP FROM PROPERTY NAME TO (MAP FROM OLD VALUE TO LIST OF OBSERVED NEW VALUES}
    "cf_blocking_b2g":{"1.5":["2.0"]}
})
EMAIL_FIELDS = {'cc', 'assigned_to', 'modified_by', 'created_by', 'qa_contact', 'bug_mentor'}

STOP_BUG = 999999999  # AN UNFORTUNATE SIDE EFFECT OF DATAFLOW PROGRAMMING (http://en.wikipedia.org/wiki/Dataflow_programming)


class BugHistoryParser(object):
    def __init__(self, settings, alias_analyzer, output_queue):
        self.startNewBug(wrap({"bug_id": 0, "modified_ts": 0, "_merge_order": 1}))
        self.prevActivityID = Null
        self.prev_row = Null
        self.settings = settings
        self.output = output_queue
        self.alias_analyzer = alias_analyzer

        if not isinstance(alias_analyzer, AliasAnalyzer):
            Log.error("expecting an AliasAnalyzer")

    def processRow(self, row_in):
        if not row_in:
            return
        try:
            self.currBugID = row_in.bug_id
            if self.settings.debug:
                Log.note("process row: {{row}}", row=row_in)

            # If we have switched to a new bug
            if self.prevBugID < self.currBugID:
                if self.prevBugID > 0:
                    # Start replaying versions in ascending order to build full data on each version
                    if DEBUG_STATUS:
                        Log.note("[Bug {{bug_id}}]: Emitting intermediate versions", bug_id=self.prevBugID)
                    self.populateIntermediateVersionObjects()
                if row_in.bug_id == STOP_BUG:
                    return
                self.startNewBug(row_in)
                if DEBUG_MEMORY and not PYPY:
                    import objgraph

                    result = objgraph.growth()
                    if result:
                        width = max(len(name) for name, _, _ in result)
                        Log.note("objgraph.growth:\n{{data}}", data="\n".join('%-*s%9d %+9d' % (width, name, count, delta) for name, count, delta in result))

            # Bugzilla bug workaround - some values were truncated, introducing uncertainty / errors:
            # https://bugzilla.mozilla.org/show_bug.cgi?id=55161
            if row_in.field_name in TRUNC_FIELDS:
                added = convert.value2string(row_in.new_value)
                removed = convert.value2string(row_in.old_value)
                uncertain = False

                if added in ["? ?", "?"]: # Unknown value extracted from a possibly truncated field
                    uncertain = True
                    Log.note("[Bug {{bug_id}}]: PROBLEM Encountered uncertain added value.  Skipping.", bug_id=self.currBugID)
                    row_in.new_value = Null
                elif added != None and added.startswith("? "): # Possibly truncated value extracted from a possibly truncated field
                    uncertain = True
                    row_in.new_value = added[2:]

                if removed in ["? ?", "?"]:# Unknown value extracted from a possibly truncated field
                    uncertain = True
                    Log.note("[Bug {{bug_id}}]: PROBLEM Encountered uncertain removed value.  Skipping.", bug_id=self.currBugID)
                    row_in.old_value = Null
                elif removed != None and removed.startswith("? "): # Possibly truncated value extracted from a possibly truncated field
                    uncertain = True
                    row_in.old_value = removed[2:]

                if uncertain and self.currBugState.uncertain == None:
                    # Process the "uncertain" flag as an activity
                    # WE ARE GOING BACKWARDS IN TIME, SO MARKUP PAST
                    Log.note("[Bug {{bug_id}}]: PROBLEM Setting this bug to be uncertain.", bug_id=self.currBugID)
                    self.processBugsActivitiesTableItem(wrap({
                        "modified_ts": row_in.modified_ts,
                        "modified_by": row_in.modified_by,
                        "field_name": "uncertain",
                        "new_value": Null,
                        "old_value": "1",
                        "attach_id": Null
                    }))
                    if row_in.new_value == None and row_in.old_value == None:
                        Log.note("[Bug {{bug_id}}]: Nothing added or removed. Skipping update.", bug_id=self.currBugID)
                        return

            # Treat timestamps as int values
            new_value = convert.value2int(row_in.new_value) if row_in.field_name.endswith("_ts") else row_in.new_value


            # Determine where we are in the bug processing workflow
            if row_in._merge_order == 1:
                self.processSingleValueTableItem(row_in.field_name, new_value)
            elif row_in._merge_order == 2:
                self.processMultiValueTableItem(row_in.field_name, new_value)
            elif row_in._merge_order == 7:
                self.processAttachmentsTableItem(row_in)
            elif row_in._merge_order == 8:
                self.processFlagsTableItem(row_in)
            elif row_in._merge_order == 9:
                self.processBugsActivitiesTableItem(row_in)
            else:
                Log.warning("Unhandled merge_order: {{order|quote}}", order=row_in._merge_order)

        except Exception as e:
            Log.warning("Problem processing row: {{row}}", row=row_in, cause=e)
        finally:
            if row_in._merge_order > 1 and self.currBugState.created_ts == None:
                Log.note("PROBLEM expecting a created_ts (did you install the timezone database into your MySQL instance?)", bug_id=self.currBugID)

            for b in self.currBugState.blocked:
                if isinstance(b, text_type):
                    Log.note("PROBLEM error {{bug_id}}", bug_id=self.currBugID)
            self.prev_row = row_in

    @staticmethod
    def uid(bug_id, modified_ts):
        if modified_ts == None:
            Log.error("modified_ts can not be Null")

        return text_type(bug_id) + "_" + text_type(modified_ts)[0:-3]

    def startNewBug(self, row_in):
        self.prevBugID = row_in.bug_id
        self.bugVersions = FlatList()
        self.bugVersionsMap = Data()
        self.currActivity = Data()
        self.currBugAttachmentsMap = {}
        self.currBugState = Data(
            _id=BugHistoryParser.uid(row_in.bug_id, row_in.modified_ts),
            bug_id=row_in.bug_id,
            modified_ts=row_in.modified_ts,
            modified_by=row_in.modified_by,
            reported_by=row_in.modified_by,
            attachments=[],
            flags=[]
        )

        #WE FORCE ADD ALL SETS, AND WE WILL scrub() THEM OUT LATER IF NOT USED
        for f in MULTI_FIELDS:
            self.currBugState[f] = set()

        if row_in._merge_order != 1:
            # Problem: No entry found in the 'bugs' table.
            Log.warning("Current bugs table record not found for bug_id: {{bug_id}}  (merge order should have been 1, but was {{start_time}})", **row_in)

    def processSingleValueTableItem(self, field_name, new_value):
        self.currBugState[field_name] = self.canonical(field_name, new_value)

    def processMultiValueTableItem(self, field_name, new_value):
        if field_name in NUMERIC_FIELDS:
            new_value = int(new_value)
        try:
            self.currBugState[field_name].add(new_value)
            return Null
        except Exception as e:
            Log.warning(
                "Unable to push {{value}} to array field {{start_time}} on bug {{curr_value}} current value: {{curr_value}}",
                value=new_value,
                field=field_name,
                bug_id=self.currBugID,
                curr_value=self.currBugState[field_name],
                cause=e
            )

    def processAttachmentsTableItem(self, row_in):
        currActivityID = BugHistoryParser.uid(self.currBugID, row_in.modified_ts)
        if currActivityID != self.prevActivityID:
            self.prevActivityID = currActivityID

            self.currActivity = Data(
                _id=currActivityID,
                modified_ts=row_in.modified_ts,
                modified_by=row_in.modified_by,
                changes=[{
                    "field_name": "attachment_added",
                    "attach_id": row_in.attach_id
                }]
            )

            if not self.currActivity.modified_ts:
                Log.error("should not happen")
            self.bugVersions.append(self.currActivity)
            self.bugVersionsMap[currActivityID] = self.currActivity

        att = self.currBugAttachmentsMap.get(row_in.attach_id)
        if att is None:
            att = Data(
                attach_id=row_in.attach_id,
                modified_ts=row_in.modified_ts,
                created_ts=row_in.created_ts,
                modified_by=row_in.modified_by,
                flags=[]
            )
            self.currBugAttachmentsMap[row_in.attach_id] = att

        att["created_ts"] = MIN([row_in.modified_ts, att["created_ts"]])
        if row_in.field_name == "created_ts" and row_in.new_value == None:
            pass
        else:
            att[row_in.field_name] = row_in.new_value

    def processFlagsTableItem(self, row_in):
        flag = parse_flag(row_in.new_value, row_in.modified_ts, row_in.modified_by)
        if row_in.attach_id != None:
            if self.currBugAttachmentsMap.get(row_in.attach_id) == None:
                if DEBUG_MISSING_ATTACHMENTS:
                    Log.note(
                        "[Bug {{bug_id}}]: Unable to find attachment {{attach_id}} for bug_id {{bug_id}}",
                        attach_id=row_in.attach_id,
                        bug_id=self.currBugID
                    )
            else:
                self.currBugAttachmentsMap[row_in.attach_id].flags.append(flag)
        else:
            self.currBugState.flags.append(flag)

    def processBugsActivitiesTableItem(self, row_in):
        if self.currBugState.created_ts == None:
            Log.error("must have created_ts")

        if row_in.field_name == "flagtypes_name":
            row_in.field_name = "flags"

        multi_field_new_value = parseMultiField(row_in.field_name, row_in.new_value)
        multi_field_old_value = parseMultiField(row_in.field_name, row_in.old_value)

        currActivityID = BugHistoryParser.uid(self.currBugID, row_in.modified_ts)
        if currActivityID != self.prevActivityID:
            self.currActivity = self.bugVersionsMap[currActivityID]
            if self.currActivity == None:
                self.currActivity = Data(
                    _id=currActivityID,
                    modified_ts=row_in.modified_ts,
                    modified_by=row_in.modified_by,
                    changes=[]
                )
                if not self.currActivity.modified_ts:
                    Log.error("should not happen")
                self.bugVersions.append(self.currActivity)

            self.prevActivityID = currActivityID

        if row_in.attach_id != None:
            attachment = self.currBugAttachmentsMap.get(row_in.attach_id)
            if attachment == None:
                # THIS HAPPENS WHEN ATTACHMENT IS PRIVATE
                pass
            else:
                if row_in.field_name == "flags":
                    total = attachment[row_in.field_name]
                    total = self.processFlags(total, multi_field_old_value, multi_field_new_value, row_in.modified_ts, row_in.modified_by, "attachment", attachment)
                    attachment[row_in.field_name] = total
                elif row_in.field_name in MULTI_FIELDS:
                    total = attachment[row_in.field_name]
                    # Can have both added and removed values.
                    total = self.removeValues(total, multi_field_new_value, "added", row_in.field_name, "attachment", attachment)
                    total = self.addValues(total, multi_field_old_value, "removed attachment", row_in.field_name, attachment)
                    attachment[row_in.field_name] = total
                else:
                    attachment[row_in.field_name] = row_in.old_value
                    self.currActivity.changes.append({
                        "field_name": row_in.field_name,
                        "new_value": row_in.new_value,
                        "old_value": row_in.old_value,
                        "attach_id": row_in.attach_id
                    })

        else:
            if row_in.field_name == "flags":
                # PROBLEM: WHEN GOING BACK IN HISTORY, AND THE ADDED VALUE IS NOT FOUND IN THE CURRENT
                # STATE, IT IS STILL RECORDED (see above self.currActivity.changes.append...).  THIS MEANS
                # WHEN GOING THROUGH THE CHANGES IN IN ORDER THE VALUE WILL EXIST, BUT IT SHOULD NOT
                total = self.currBugState[row_in.field_name]
                total = self.processFlags(total, multi_field_old_value, multi_field_new_value, row_in.modified_ts, row_in.modified_by, "bug", self.currBugState)
                self.currBugState[row_in.field_name] = total
            elif row_in.field_name in MULTI_FIELDS:
                # PROBLEM: WHEN GOING BACK IN HISTORY, AND THE ADDED VALUE IS NOT FOUND IN THE CURRENT
                # STATE, IT IS STILL RECORDED (see above self.currActivity.changes.append...).  THIS MEANS
                # WHEN GOING THROUGH THE CHANGES IN IN ORDER THE VALUE WILL EXIST, BUT IT SHOULD NOT
                total = self.currBugState[row_in.field_name]
                # Can have both added and removed values.
                total = self.removeValues(total, multi_field_new_value, "added", row_in.field_name, "currBugState", self.currBugState)
                total = self.addValues(total, multi_field_old_value, "removed bug", row_in.field_name, self.currBugState)
                self.currBugState[row_in.field_name] = total
            elif row_in.field_name in DIFF_FIELDS:
                diff = row_in.new_value
                expected_value = self.currBugState[row_in.field_name]
                try:
                    old_value = ApplyDiff(self.currBugID, row_in.modified_ts, expected_value, diff, reverse=True)
                    self.currBugState[row_in.field_name] = old_value
                    self.currActivity.changes.append({
                        "field_name": row_in.field_name,
                        "new_value": expected_value,
                        "old_value": old_value,
                        "attach_id": row_in.attach_id
                    })
                except Exception as e:
                    Log.warning(
                        "[Bug {{bug_id}}]: PROBLEM Unable to process {{field_name}} diff:\n{{diff|indent}}",
                        bug_id=self.currBugID,
                        field_name=row_in.field_name,
                        diff=diff,
                        cause=e
                    )
            elif row_in.field_name in LONG_FIELDS:
                new_value = row_in.new_value
                curr_value = self.currBugState[row_in.field_name]
                try:
                    old_value = LongField(self.currBugID, row_in.modified_ts, curr_value, row_in.old_value)
                    self.currBugState[row_in.field_name] = old_value
                    self.currActivity.changes.append({
                        "field_name": row_in.field_name,
                        "new_value": curr_value,
                        "old_value": old_value,
                        "attach_id": row_in.attach_id
                    })
                except Exception as e:
                    Log.warning(
                        "[Bug {{bug_id}}]: PROBLEM Unable to process {{field_name}} text:\n{{text|indent}}",
                        bug_id=self.currBugID,
                        field_name=row_in.field_name,
                        text=new_value,
                        cause=e
                    )
            else:
                old_value = self.canonical(row_in.field_name, row_in.old_value)

                if DEBUG_CHANGES and row_in.field_name not in KNOWN_INCONSISTENT_FIELDS:
                    expected_value = self.canonical(row_in.field_name, self.currBugState[row_in.field_name])
                    new_value = self.canonical(row_in.field_name, row_in.new_value)

                    if text_type(new_value) != text_type(expected_value):
                        if row_in.field_name in EMAIL_FIELDS:
                            if Math.is_integer(new_value) or Math.is_integer(expected_value) and row_in.modified_ts<=927814152000:
                                pass # BEFORE 1999-05-27 14:09:12 THE qa_contact FIELD WAS A NUMBER, NOT THE EMAIL
                            elif not new_value or not expected_value:
                                pass
                            else:
                                self.alias_analyzer.add_alias(lost=new_value, found=expected_value)
                        else:
                            # RECORD INCONSISTENCIES, MAYBE WE WILL FIND PATTERNS
                            expected_list = FIELDS_CHANGED[row_in.field_name][literal_field(text_type(new_value))]
                            if expected_value not in expected_list:
                                # expected_list += [expected_value]
                                # File("expected_values.json").write(value2json(FIELDS_CHANGED, pretty=True))

                                Log.note(
                                    "[Bug {{bug_id}}]: PROBLEM inconsistent change: {{field}} was {{expecting|quote}} got {{observed|quote}}",
                                    bug_id=self.currBugID,
                                    field=row_in.field_name,
                                    expecting=expected_value,
                                    observed=new_value
                                )

                # WE DO NOT ATTEMPT TO CHANGE THE VALUES IN HISTORY TO BE CONSISTENT WITH THE FUTURE
                self.currActivity.changes.append({
                    "field_name": row_in.field_name,
                    "new_value": self.currBugState[row_in.field_name],
                    "old_value": old_value,
                    "attach_id": row_in.attach_id
                })
                self.currBugState[row_in.field_name] = old_value

    def populateIntermediateVersionObjects(self):
        # Make sure the self.bugVersions are in descending order by modification time.
        # They could be mixed because of attachment activity
        self.bugVersions = jx.sort(self.bugVersions, [
            {"field": "modified_ts", "sort": -1}
        ])

        # Tracks the previous distinct value for field
        prevValues = {}
        currVersion = Null
        # Prime the while loop with an empty next version so our first iteration outputs the initial bug state
        nextVersion = Data(_id=self.currBugState._id, changes=[])

        # A monotonically increasing version number (useful for debugging)
        self.bug_version_num = 1

        # continue if there are more bug versions, or there is one final nextVersion
        while nextVersion:
            try:
                currVersion = nextVersion
                if self.bugVersions:
                    try:
                        nextVersion = self.bugVersions.pop() # Oldest version
                        if nextVersion.modified_ts > self.settings.end_time:
                            if DEBUG_STATUS:
                                Log.note(
                                    "[Bug {{bug_id}}]: Not outputting {{_id}} - it is after self.end_time ({{end_time|datetime}})",
                                    _id=nextVersion._id,
                                    end_time=self.settings.end_time,
                                    bug_id=self.currBugState.bug_id
                                )
                            nextVersion = Null
                    except Exception as e:
                        Log.error("problem", e)
                else:
                    nextVersion = Null

                if DEBUG_STATUS:
                    Log.note("[Bug {{bug_id}}]: Populating JSON for version {{id}}", {
                        "id": currVersion._id,
                        "bug_id": self.currBugState.bug_id
                    })
                # Decide whether to merge this bug activity into the current state (without emitting
                # a separate JSON document). This addresses the case where an attachment is created
                # at exactly the same time as the bug itself.
                # Effectively, we combine all the changes for a given timestamp into the last one.
                mergeBugVersion = False
                if nextVersion != None and currVersion._id == nextVersion._id:
                    if DEBUG_STATUS:
                        Log.note(
                            "[Bug {{bug_id}}]: Merge mode: activated {{id}}",
                            id=self.currBugState._id,
                            bug_id=self.currBugState.bug_id
                        )
                    mergeBugVersion = True

                # Link this version to the next one (if there is a next one)
                self.currBugState.expires_on = coalesce(nextVersion.modified_ts, MAX_TIMESTAMP)

                # Copy all attributes from the current version into self.currBugState
                for propName, propValue in currVersion.items():
                    self.currBugState[propName] = propValue
                # self.currBugState.previous_values = self.currBugState.previous_values.copy()

                # Now walk self.currBugState forward in time by applying the changes from currVersion
                # BE SURE TO APPLY REMOVES BEFORE ADDS, JUST IN CASE BOTH HAPPENED TO ONE FIELD
                changes = jx.sort(currVersion.changes, ["attach_id", "field_name", {"field": "old_value", "sort": -1}, "new_value"])
                self.currBugState.changes = currVersion.changes = changes

                for c, change in enumerate(changes):
                    if change.old_value == change.new_value and not change.attach_id:
                        # THIS HAPPENS FOR LONG FIELDS AND DIFF FIELDS
                        changes[c] = Null
                        continue
                    if c + 1 < len(changes):
                        # PACK ADDS AND REMOVES TO SINGLE CHANGE TO MATCH ORIGINAL
                        next = changes[c + 1]
                        if change.attach_id == next.attach_id and change.field_name == next.field_name:
                            if change.new_value == next.old_value:
                                next.old_value = change.old_value
                                changes[c] = Null
                                continue

                            if not is_null(change.old_value) and is_null(next.old_value):
                                next.old_value = change.old_value
                                change.old_value = Null
                            elif not is_null(change.new_value) and is_null(next.new_value):
                                next.new_value = change.new_value
                                change.new_value = Null

                        if (
                            is_null(change.new_value) and
                            is_null(change.old_value) and
                            change.field_name != "attachment_added"
                        ):
                            changes[c] = Null
                            continue

                    target = self.currBugState
                    targetName = "currBugState"
                    attach_id = change.attach_id
                    if attach_id != None:
                        # Handle the special change record that signals the creation of the attachment
                        if change.field_name == "attachment_added":
                            # This change only exists when the attachment has been added to the map, so no missing case needed.
                            att = self.currBugAttachmentsMap[attach_id]
                            self.currBugState.attachments.append(att)
                            continue
                        else:
                            # Attachment change
                            target = self.currBugAttachmentsMap.get(attach_id)
                            targetName = "attachment"
                            if target == None:
                                if DEBUG_MISSING_ATTACHMENTS:
                                    Log.note("[Bug {{bug_id}}]: Encountered a change to missing attachment: {{change}}", {
                                        "bug_id": self.currBugState.bug_id,
                                        "change": change
                                    })

                                # treat it as a change to the main bug instead :(
                                target = self.currBugState
                                targetName = "currBugState"

                    if change.field_name == "flags":
                        self.processFlagChange(target, change, currVersion.modified_ts, currVersion.modified_by)
                    elif change.field_name in MULTI_FIELDS:
                        a = target[change.field_name]
                        multi_field_value = change.new_value
                        multi_field_value_removed = change.old_value

                        # This was a deletion, find and delete the value(s)
                        a = self.removeValues(a, multi_field_value_removed, "removed", change.field_name, targetName, target)
                        # Handle addition(s) (if any)
                        a = self.addValues(a, multi_field_value, "added", change.field_name, target)
                        target[change.field_name] = a
                    else:
                        # Simple field change.
                        # Track the previous value
                        # Single-value field has changed in bug or attachment
                        # Make sure its actually changing.  We seem to get change
                        # entries for attachments that show the current field value.
                        if target[change.field_name] != change.new_value:
                            self.setPrevious(target, change.field_name, target[change.field_name], currVersion.modified_ts)

                        target[change.field_name] = change.new_value

                self.currBugState.bug_version_num = self.bug_version_num

                if not mergeBugVersion:
                    # This is not a "merge", so output a row for this bug version.
                    self.bug_version_num += 1
                    state = normalize(self.currBugState)

                    try:
                        value2json(state)
                    except Exception as e:
                        Log.error("problem with {{bug}}", bug=state.bug_id, cause=e)

                    if DEBUG_STATUS:
                        Log.note("[Bug {{bug_state.bug_id}}]: v{{bug_state.bug_version_num}} (id = {{bug_state.id}})", bug_state=state)
                    self.output.add({"id": state.id, "value": state})  #ES EXPECTED FORMAT
                else:
                    if DEBUG_STATUS:
                        Log.note("[Bug {{bug_state.bug_id}}]: Merging a change with the same timestamp = {{bug_state._id}}: {{bug_state}}", bug_state=currVersion)
            finally:
                if self.currBugState.blocked == None:
                    Log.note("[Bug {{bug_id}}]: expecting a created_ts", bug_id= currVersion.bug_id)
                pass

    def findFlag(self, flag_list, flag):
        for f in flag_list:
            if (
                f.request_type and flag.request_type and
                deformat(f.request_type) == deformat(flag.request_type) and
                f.request_status == flag.request_status and
                (
                    (f.request_status!='?' and self.email_alias(f.modified_by) == self.email_alias(flag.modified_by)) or
                    (f.request_status=='?' and self.email_alias(f.requestee) == self.email_alias(flag.requestee))
                )
            ):
                return f

        for f in flag_list:
            if f.value == flag.value:
                return f  # PROBABLY NEVER HAPPENS, IF THE FLAG CAN'T BE MATCHED, IT'S BECAUSE IT CAN'T BE PARSED, WHICH IS BECAUSE IT HAS BEEN CHOPPED OFF BY THE 255 CHAR LIMIT IN BUGS_ACTIVIY TABLE

        # BUGS_ACTIVITY HAS LOTS OF GARBAGE (255 CHAR LIMIT WILL CUT OFF REVIEW REQUEST LISTS)
        # TRY A LESS STRICT MATCH
        for f in flag_list:
            min_len=min(len(f.value), len(flag.value))
            if f.value[:min_len] == flag.value[:min_len]:
                return f

        return Null


    def processFlagChange(self, target, change, modified_ts, modified_by):
        target.flags = listwrap(target.flags)

        added_flags, change.new_value = change.new_value, set(c.value for c in change.new_value)
        removed_flags, change.old_value = change.old_value, set(c.value for c in change.old_value)

        # First, mark any removed flags as straight-up deletions.
        for removed_flag in removed_flags:
            existing_flag = self.findFlag(target.flags, removed_flag)

            if existing_flag:
                # Carry forward some previous values:
                existing_flag["previous_modified_ts"] = existing_flag["modified_ts"]
                existing_flag["modified_ts"] = modified_ts
                if existing_flag["modified_by"] != modified_by:
                    existing_flag["previous_modified_by"] = existing_flag["modified_by"]
                    existing_flag["modified_by"] = modified_by

                # Add changed stuff:
                existing_flag["previous_status"] = removed_flag["request_status"]
                existing_flag["request_status"] = "d"
                existing_flag["previous_value"] = removed_flag.value
                existing_flag["value"] = Null  # SPECIAL INDICATOR FOR DELETED FLAG
                # request_type stays the same.
                # requestee stays the same.

                duration_ms = existing_flag["modified_ts"] - existing_flag["previous_modified_ts"]
                # existingFlag["duration_days"] = math.floor(duration_ms / (1000.0 * 60 * 60 * 24))  # TODO: REMOVE floor
            else:
                self.findFlag(target.flags, removed_flag)
                Log.note(
                    "[Bug {{bug_id}}]: PROBLEM: Did not find removed FLAG {{removed}} in {{existing}}",
                    removed=removed_flag.value,
                    existing=target.flags,
                    bug_id=self.currBugState.bug_id
                )

        # See if we can align any of the added flags with previous deletions.
        # If so, try to match them up with a "dangling" removed flag
        for added_flag in added_flags:
            candidates = wrap([
                unwrap(element)
                for element in target.flags
                if (
                    element["value"] == None  # SPECIAL INDICATOR FOR DELETED FLAG
                    and added_flag["request_type"] == element["request_type"]
                    and added_flag["request_status"] != element["previous_status"]  # Skip "r?(dre@mozilla)" -> "r?(mark@mozilla)"
                )
            ])

            if not candidates:
                # No matching candidate. Totally new flag.
                target.flags.append(added_flag)
                continue

            chosen_one = candidates[0]
            if len(candidates) > 1:
                # Multiple matches - use the best one.
                if DEBUG_FLAG_MATCHES:
                    Log.note(
                        "[Bug {{bug_id}}]: Matched added flag {{flag}} to multiple removed flags {{candidates}}.  Finding the best...",
                        flag=added_flag,
                        candidates=candidates,
                        bug_id=self.currBugState.bug_id
                    )

                matched_ts = [
                    element
                    for element in candidates
                    if added_flag.modified_ts == element.modified_ts
                ]

                matched_req = [
                    element
                    for element in candidates
                    if self.email_alias(added_flag["modified_by"]) == self.email_alias(element["requestee"])
                ]

                if not matched_ts and not matched_req:
                    # No matching candidate. Totally new flag.
                    target.flags.append(added_flag)
                    continue
                elif len(matched_ts) == 1 or (not matched_req and matched_ts):
                    chosen_one = matched_ts[0]
                    if DEBUG_FLAG_MATCHES:
                        Log.note(
                            "[Bug {{bug_id}}]: Matching on modified_ts:\n{{best|indent}}",
                            bug_id=self.currBugState.bug_id,
                            best=chosen_one
                        )
                elif not matched_ts and matched_req:
                    chosen_one = matched_req[0]  #PICK ANY
                    if DEBUG_FLAG_MATCHES:
                        Log.note(
                            "[Bug {{bug_id}}]: Matching on requestee",
                            bug_id=self.currBugState.bug_id,
                            best=chosen_one
                        )
                else:
                    matched_both = [
                        element
                        for element in candidates
                        if added_flag.modified_ts == element.modified_ts and self.email_alias(added_flag["modified_by"]) == self.email_alias(element["requestee"])
                    ]

                    if matched_both:
                        if DEBUG_FLAG_MATCHES:
                            Log.note("[Bug {{bug_id}}]: Matching on modified_ts and requestee fixed it", bug_id=self.currBugState.bug_id)
                        chosen_one = matched_both[0]  #PICK ANY
                    else:
                        if DEBUG_FLAG_MATCHES:
                            Log.note("[Bug {{bug_id}}]: Matching on modified_ts fixed it", bug_id=self.currBugState.bug_id)
                        chosen_one = matched_ts[0]
            else:
                # Obvious case - matched exactly one.
                if DEBUG_STATUS:
                    Log.note(
                        "[Bug {{bug_id}}]: Matched added flag {{added}} to removed flag {{removed}}",
                        added=added_flag,
                        removed=chosen_one,
                        bug_id=self.currBugState.bug_id
                    )

            if chosen_one != None:
                for f in ["value", "request_status", "requestee"]:
                    chosen_one[f] = coalesce(added_flag[f], chosen_one[f])

                    # We need to avoid later adding this flag twice, since we rolled an add into a delete.


    def setPrevious(self, dest, field_name, previous_value, change_ts):
        if dest["previous_values"] == None:
            dest["previous_values"] = {}
        pv = dest["previous_values"]

        if USE_PREVIOUS_VALUE_OBJECTS:
            prev_field_name = field_name + ".value"
            caField = field_name + ".end_time"
            ctField = field_name + ".start_time"
            ddField = Null
        else:
            prev_field_name = field_name + "_value"
            caField = field_name + "_change_away_ts"
            ctField = field_name + "_change_to_ts"
            ddField = field_name + "_duration_days"

        pv[prev_field_name] = previous_value
        # If we have a previous change for this field, then use the
        # change-away time as the new change-to time.
        if pv[caField] != None:
            pv[ctField] = pv[caField]
        else:
            # Otherwise, this is the first change for this field, so
            # use the creation timestamp.
            pv[ctField] = dest["created_ts"]

        pv[caField] = change_ts
        try:
            duration_ms = pv[caField] - pv[ctField]
            pv[ddField] = math.floor(duration_ms / (1000.0 * 60 * 60 * 24))
        except Exception as e:
            Log.error("", e)

    def addValues(self, total, add, valueType, field_name, target):
        if not add:
            return total
            #        Log.note("[Bug {{bug_id}}]: Adding " + valueType + " " + fieldName + " values:" + value2json(someValues))
        if field_name == "flags":
            Log.error("use processFlags")
        else:
            diff = add - total
            removed = total & add

            #WE CAN NOT REMOVE VALUES WE KNOW TO BE THERE AFTER
            if removed and (field_name != 'cc' or DEBUG_CC_CHANGES) and field_name not in KNOWN_MISSING_KEYWORDS:
                Log.note(
                    "[Bug {{bug_id}}]: PROBLEM: Found {{type}} {{field_name}} value: (Removing {{removed}} can not result in {{existing}})",
                    bug_id= target.bug_id,
                    type=valueType,
                    field_name=field_name,
                    removed=removed,
                    existing=target[field_name]
                )

            if valueType != "added" and diff:
                self.currActivity.changes.append({
                    "field_name": field_name,
                    "new_value": set(),
                    "old_value": diff,
                    "attach_id": target.attach_id
                })

            return total | add


    def removeValues(self, total, remove, valueType, field_name, arrayDesc, target):
        if field_name == "flags":
            Log.error("use processFlags")
        elif field_name == "cc":
            # MAP CANONICAL TO EXISTING (BETWEEN map_* AND self.email_aliases WE HAVE A BIJECTION)
            map_total = inverse({t: self.email_alias(t) for t in total})
            map_remove = inverse({r: self.email_alias(r) for r in remove})
            # CANONICAL VALUES
            c_total = set(map_total.keys())
            c_remove = set(map_remove.keys())

            removed = c_total & c_remove
            diff = c_remove - c_total
            output = c_total - c_remove

            if not target.uncertain:
                if diff and DEBUG_CC_CHANGES:
                    Log.note("[Bug {{bug_id}}]: PROBLEM: Unable to find CC:\n{{missing|indent}}\nnot in:\n{{existing|indent}}\ncurrent alias info:\n{{candidates|indent}}", {
                        "type": valueType,
                        "object": arrayDesc,
                        "field_name": field_name,
                        "missing": jx.sort(jx.map2set(diff, map_remove)),
                        "existing": jx.sort(total),
                        "candidates": {d: self.email_aliases.get(d, None) for d in diff},
                        "bug_id": self.currBugID
                    })
            else:
                # PATTERN MATCH EMAIL ADDRESSES
                # self.cc_list_ok = False
                for lost in diff:
                    best_score = 0.3
                    best = Null
                    for found in output:
                        score = MIN([
                            strings.edit_distance(found, lost),
                            strings.edit_distance(found.split("@")[0], lost.split("@")[0]),
                            strings.edit_distance(map_total[found][0], lost),
                            strings.edit_distance(map_total[found][0].split("@")[0], lost.split("@")[0])
                        ])
                        if score < best_score:
                            # best_score=score
                            best = found

                    if best != Null:
                        if DEBUG_CC_CHANGES:
                            Log.note("[Bug {{bug_id}}]: UNCERTAIN ALIAS FOUND: {{lost}} == {{found}}", {
                                "lost": lost,
                                "found": best,
                                "bug_id": self.currBugID
                            })
                            #DO NOT SAVE THE ALIAS, IT MAY BE WRONG
                        removed.add(best)
                        output.discard(best)
                    elif DEBUG_CC_CHANGES:
                        Log.note("[Bug {{bug_id}}]: PROBLEM Unable to pattern match {{type}} value: {{object}}.{{field_name}}: ({{missing}}" + " not in : {{existing}})", {
                            "type": valueType,
                            "object": arrayDesc,
                            "field_name": field_name,
                            "missing": lost,
                            "existing": total,
                            "bug_id": self.currBugID
                        })

            if valueType == "added":
                # DURING WALK BACK IN TIME, WE POPULATE THE changes
                try:
                    if removed - set(map_total.keys()):
                        Log.error("problem with alias finding:\n" +
                                  "map_total={{map_total}}\n" +
                                  "map_remove={{map_remove}}\n" +
                                  "c_total={{c_total}}\n" +
                                  "c_remove={{c_remove}}\n" +
                                  "removed={{removed}}\n" +
                                  "diff={{diff}}\n" +
                                  "output={{output}}\n", {
                            "map_total": map_total,
                            "c_total": c_total,
                            "map_remove": map_remove,
                            "c_remove": c_remove,
                            "removed": removed,
                            "diff": diff,
                            "output": output
                        })
                    final_removed = jx.map2set(removed, map_total)
                    if final_removed:
                        self.currActivity.changes.append({
                            "field_name": field_name,
                            "new_value": final_removed,
                            "old_value": set(),
                            "attach_id": target.attach_id
                        })
                except Exception as email:
                    Log.error("issues", email)

            return jx.map2set(output, map_total)
        else:
            removed = total & remove
            diff = remove - total
            output = total - remove

            if valueType == "added" and removed:
                self.currActivity.changes.append({
                    "field_name": field_name,
                    "new_value": removed,
                    "old_value": set(),
                    "attach_id": target.attach_id
                })

            if diff and field_name not in ['blocked', 'dependson']:  # HAPPENS BECAUSE OF MISSING PRIVATE BUGS
                Log.note("[Bug {{bug_id}}]: PROBLEM Unable to find {{type}} value in {{object}}.{{field_name}}: (All {{missing}}" + " not in : {{existing}})", {
                    "bug_id": target.bug_id,
                    "type": valueType,
                    "object": arrayDesc,
                    "field_name": field_name,
                    "missing": diff,
                    "existing": total
                })
                if field_name == "keywords":
                    KNOWN_MISSING_KEYWORDS.update(diff)

            return output

    def processFlags(self, total, old_values, new_values, modified_ts, modified_by, target_type, target):
        added_values = [] #FOR SOME REASON, REMOVAL BY OBJECT DOES NOT WORK, SO WE USE THIS LIST OF STRING  VALUES
        for v in new_values:
            flag = parse_flag(v, modified_ts, modified_by)

            if flag.request_type == None:
                Log.note("[Bug {{bug_id}}]: PROBLEM Unable to parse flag {{flag}} (caused by 255 char limit?)", {
                    "flag": convert.value2quote(flag.value),
                    "bug_id": self.currBugID
                })
                continue

            found = self.findFlag(total, flag)
            if found:
                before=len(total)
                total.remove(found)
                after = len(total)
                if before != after+1:
                    Log.error("")
                # total = wrap([unwrap(a) for a in total if tuple(a.items()) != tuple(found.items())])  # COMPARE DICTS
                added_values.append(flag)
            else:
                Log.note(
                    "[Bug {{bug_id}}]: PROBLEM Unable to find {{type}} FLAG: {{object}}.{{field_name}}: (All {{missing}}" + " not in : {{existing}})",
                    type=target_type,
                    object=coalesce(target.attach_id, target.bug_id),
                    field_name="flags",
                    missing=v,
                    existing=total,
                    bug_id=self.currBugID
                )

        if added_values:
            self.currActivity.changes.append({
                "field_name": "flags",
                "new_value": added_values,
                "old_value": [],
                "attach_id": target.attach_id
            })

        if old_values:
            removed_values = [
                parse_flag(v, modified_ts, modified_by)
                for v in old_values
            ]
            total.extend(removed_values)

            self.currActivity.changes.append({
                "field_name": "flags",
                "new_value": [],
                "old_value": removed_values,
                "attach_id": target.attach_id
            })

        return total

    def canonical(self, field, value):
        try:
            if value in NULL_VALUES:
                return None
            elif field in EMAIL_FIELDS:
                return self.email_alias(value)
            elif field in TIME_FIELDS:
                value = long(Date(value).unix) * 1000
            elif field in NUMERIC_FIELDS:
                value = value2number(value)

            # candidates = FIELDS_CHANGED[field][literal_field(str(value))]
            # if candidates == None:
            #     return value
            # elif len(candidates) == 1:
            #     return candidates[0]
            # else:
            return value
        except Exception:
            return value


    def email_alias(self, name):
        return self.alias_analyzer.get_canonical(name)


def parse_flag(flag, modified_ts, modified_by):
    flagParts = Data(
        modified_ts=modified_ts,
        modified_by=modified_by,
        value=flag
    )

    matches = FLAG_PATTERN.match(flag)
    if matches:
        flagParts.request_type = matches.group(1)
        flagParts.request_status = matches.group(2)
        if matches.start(3) != -1 and len(matches.group(3)) > 2:
            flagParts.requestee = matches.group(3)[1:-1]

    return flagParts


def parseMultiField(name, value):
    if name == "flags":
        if value == None:
            return []
        else:
            return list(s.strip() for s in value.split(",") if s.strip() != "")
    elif value == None:
        return set()
    elif isinstance(value, (list, set)):
        Log.error("do not parse lists")
    elif name in MULTI_FIELDS:
        if name in NUMERIC_FIELDS:
            return set(int(s.strip()) for s in value.split(",") if s.strip() != "")
        else:
            return set(s.strip() for s in value.split(",") if s.strip() != "")

    return {value}


def deformat(value):
    if value == None:
        Log.error("not expected")
    return value.lower().replace(u"\u2011", u"-")


def is_null(value):
    if value == None:
        return True
    if isinstance(value, (set, list)):
        return len(value)==0
    return False


class ApplyDiff(object):

    def __init__(self, bug_id, timestamp, text, diff, reverse=None):
        """
        THE BUGZILLA DIFF IS ACROSS MULTIPLE RECORDS, THEY MUST BE APPENDED TO MAKE THE DIFF
        :param timestamp: DATABASE bug_activity TIMESTAMP THAT WILL BE THE SAME FOR ALL IN A HUNK
        :param text: THE ORIGINAL TEXT (OR A PROMISE OF TEXT)
        :param diff: THE PARTITAL DIFF
        :param reverse: DIRECTION TO APPLY THE DIFF
        :return: A PROMISE TO RETURN THE diff APPLIED TO THE text
        """
        self.bug_id = bug_id
        self.timestamp = timestamp
        self._text = coalesce(text, "")
        self._diff = diff
        self.reverse = reverse
        self.parent = None
        self.result = None

        if isinstance(text, ApplyDiff):
            if text.timestamp != timestamp:
                # DIFFERNT DIFF
                self._text = str(text) # ACTUALIZE THE EFFECTS OF THE OTHER DIFF
            else:
                # CHAIN THE DIFF
                text.parent = self
                text.parent.result = None  # JUST IN CASE THIS HAS BEEN ACTUALIZED

    @property
    def text(self):
        if isinstance(self._text, ApplyDiff):
            return self._text.text
        else:
            return self._text

    @property
    def diff(self):
        # WHEN GOING BACKWARDS IN TIME, THE DIFF WILL ARRIVE IN REVERSE ORDER
        # LUCKY THAT THE STACK OF DiffApply REVERSES THE REVERSE ORDER
        if isinstance(self._text, ApplyDiff):
            return self._diff + self._text.diff
        else:
            return self._diff

    def __data__(self):
        return self.__str__()

    def __gt__(self, other):
        return str(self)>other

    def __lt__(self, other):
        return str(self)<other

    def __eq__(self, other):
        if other == None:
            return False  # DO NOT ACTUALIZE
        return str(self)==other

    def __str__(self):
        if self.parent:
            return str(self.parent)

        text = self.text
        diff = self.diff
        if not self.result:
            try:
                self.result = "\n".join(apply_diff(text.split("\n"), diff.split("\n"), reverse=self.reverse, verify=DEBUG_DIFF))
            except Exception as e:
                e = Except.wrap(e)
                self.result = "<ERROR>"
                Log.warning("problem applying diff for bug {{bug}}", bug=self.bug_id, cause=e)

        return self.result


class LongField(object):

    def __init__(self, bug_id, timestamp, next_value, text):
        """
        THE BUGZILLA LONG FIELDS ARE ACROSS MULTIPLE RECORDS, THEY MUST BE APPENDED
        :param timestamp: DATABASE bug_activity TIMESTAMP THAT WILL BE THE SAME FOR ALL IN A HUNK
        :param next_value: THE ORIGINAL TEXT (OR A PROMISE OF TEXT)
        :param text: THE PARTITAL CONTENT
        :return: A PROMISE TO RETURN THE FULL TEXT
        """
        self.bug_id = bug_id
        self.timestamp = timestamp
        self.value = text
        self.prev_value = None
        self.next_value = None

        if isinstance(next_value, LongField) and next_value.timestamp == timestamp:
            # CHAIN THE DIFF
            self.next_value = next_value
            next_value.prev_value = self

    @property
    def text(self):
        # WHEN GOING BACKWARDS IN TIME, THE DIFF WILL ARRIVE IN REVERSE ORDER
        # LUCKY THAT THE STACK OF DiffApply REVERSES THE REVERSE ORDER
        if self.next_value is not None:
            return self.value + self.next_value.text
        else:
            return self.value

    def __data__(self):
        return text_type(self)

    def __gt__(self, other):
        return text_type(self) > text_type(other)

    def __lt__(self, other):
        return text_type(self) < text_type(other)

    def __eq__(self, other):
        if other == None:
            return False  # DO NOT ACTUALIZE
        return text_type(self) == text_type(other)

    def __str__(self):
        if self.prev_value:
            return str(self.prev_value)
        return self.value

    def __unicode__(self):
        if self.prev_value:
            return text_type(self.prev_value)
        return self.value


# ENSURE WE REGISTER THIS PROMISE AS A STRING
meta._type_to_name[ApplyDiff] = "string"
meta._type_to_name[LongField] = "string"
