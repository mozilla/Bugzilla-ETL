#""" vim: set filetype=javascript ts=2 et sw=2: """
#""" 
#Workflow:
#Follow the same procedure as when parsing bug history, but take note of
#cases where a change relating to a user has no direct match in the bug's
#history.  This can indicate a case where a user's bugzilla id changed.
#
#TODO: One possible enhancement from deinspanjer (paraphrased by mreid):
#To verify an alias, we could try the following
#- query bugs table with newer name, "assigned_to"
#- Match the id up to get the current name, look at the bugs history to
#  check the "assigned_to" in activities (since it's stored as text)
#- If we get a "hit", that would guarantee that it is an alias.
#"""
#
##  Used to split a flag into (type, status [,requestee])
##  Example: "review?(mreid@mozilla.com)" -> (review, ?, mreid@mozilla.com)
##  Example: "review-" -> (review, -)
import re
from util.query import Q

FLAG_PATTERN=re.compile("^(.*)([?+-])(\\([^)]*\\))?$")
#
##  Used to reformat incoming dates into the expected form.
##  Example match: "2012/01/01 00:00:00.000"
DATE_PATTERN=re.compile("^[0-9]{4}\\/[0-9]{2}\\/[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}")
#
##  Fields that could have been truncated per bug 55161
TRUNC_FIELDS=["cc", "blocked", "dependson", "keywords"]
#
from util.cnv import CNV
from util.debug import D
from util.map import Map


##  Keep track of potential aliases (dupes)
##  Singles are where we have only one possible match left over (high confidence)
dupeSingles={}
##  Multis are where we have more than one possible match left over (uncertain)
dupeMultis={}
currBugID=None
prevBugID=None
bugVersions=None
bugVersionsMap=None
currBugState=None
currBugAttachmentsMap=None
prevActivityID=None
currActivity=None
inputRowSize=getInputRowMeta().size()

START_TIME=parseInt(getVariable("START_TIME", 0))
END_TIME=parseInt(getVariable("END_TIME", 0))





def processRow(bug_id, modified_ts, modified_by, field_name, field_value_in, field_value_removed, attach_id, _merge_order):

    currBugID=bug_id

    D.println("bug_id={" + bug_id + "}, modified_ts={" + modified_ts + "}, modified_by={" + modified_by \
          + "}, field_name={" + field_name + "}, field_value={" + field_value_in + "}, field_value_removed={"\
          + field_value_removed + "}, attach_id={" + attach_id + "}, _merge_order={" + _merge_order + "}")

    #  For debugging purposes:
    if END_TIME > 0 and modified_ts > END_TIME:
        D.println("l", "Skipping change after END_TIME (" + END_TIME + ")")
        return
    

    #  If we have switched to a new bug
    if prevBugID < currBugID:
        #  Start replaying versions in ascending order to build full data on each version
        D.println("Emitting intermediate versions for " + prevBugID)
        populateIntermediateVersionObjects()
        startNewBug(bug_id, modified_ts, modified_by, _merge_order)
    

    #  Bugzilla bug workaround - some values were truncated, introducing uncertainty / errors:
    #  https://bugzilla.mozilla.org/show_bug.cgi?id=55161
    if TRUNC_FIELDS.indexOf(field_name) >= 0:
        uncertain=False

       #  Unknown value extracted from a possibly truncated field
       if field_value_in == "? ?" or field_value_removed == "? ?":
          uncertain=True
          if field_value_in == "? ?":
             D.println("Encountered uncertain added value.  Skipping.")
             field_value_in=""
          
          if field_value_removed == "? ?":
             D.println("Encountered uncertain removed value.  Skipping.")
             field_value_removed=""
          
       

       #  Possibly truncated value extracted from a possibly truncated field
       if field_value_in.indexOf("? ") == 0:
          uncertain=True
          field_value_in=field_value_in[2:]
       
       if field_value_removed.indexOf("? ") == 0:
          uncertain=True
          field_value_removed=field_value_removed[2:]
       

       if uncertain:
          #  Process the "uncertain" flag as an activity
          D.println("Setting this bug to be uncertain.")
          processBugsActivitiesTableItem(modified_ts, modified_by, "uncertain", "1", "", "")
          if field_value_in == "" and field_value_removed == "":
             D.println("Nothing added or removed. Skipping update.")
             return
          
       
     

    #  Treat timestamps as int values
    field_value=int(field_value_in) if field_name.endsWith("_ts") else field_value_in

    if currBugID < 999999999:
        #  Determine where we are in the bug processing workflow
        if _merge_order== 1:
            processSingleValueTableItem(field_name, field_value)
        elif _merge_order== 2:
            processMultiValueTableItem(field_name, field_value)
        elif _merge_order== 7:
            processAttachmentsTableItem(modified_ts, modified_by, field_name, field_value, field_value_removed, attach_id)
        elif _merge_order== 8:
            processFlagsTableItem(modified_ts, modified_by, field_name, field_value, field_value_removed, attach_id)
        elif _merge_order== 9:
            processBugsActivitiesTableItem(modified_ts, modified_by, field_name, field_value, field_value_removed, attach_id)
        else:
            D.println("e", "Unhandled merge_order: '" + _merge_order + "'")
        
    


def startNewBug(bug_id, modified_ts, modified_by, merge_order) :
    if currBugID >= 999999999: return
    
    prevBugID=bug_id
    bugVersions=[]
    bugVersionsMap={}
    currActivity={}
    currBugAttachmentsMap={}
    currBugState={
        "bug_id": bug_id,
        "modified_ts": modified_ts,
        "modified_by": modified_by,
        "reported_by": modified_by,
        "attachments": [],
        "flags": []
    }
    currBugState._id=bug_id + "." + modified_ts

    if merge_order != 1:
        #  Problem: No entry found in the 'bugs' table.
        D.println("e", "Current bugs table record not found for bug_id: "\
        + bug_id + " (merge order should have been 1, but was " + merge_order + ")")
  


def processSingleValueTableItem(field_name, field_value) :
    currBugState[field_name]=field_value


def processMultiValueTableItem(field_name, field_value) :
    if currBugState[field_name] is None:
        currBugState[field_name]=[]
    
    try :
        currBugState[field_name].push(field_value)
    except Exception, e:
        D.println("e", "Unable to push " + field_value + " to array field "
            + field_name + " on bug " + currBugID + " current value:"
            + CNV.object2JSON(currBugState[field_name]))
    


def processAttachmentsTableItem(modified_ts, modified_by, field_name, field_value, field_value_removed, attach_id) :
    currActivityID=currBugID+"."+modified_ts
    if currActivityID != prevActivityID:
        currActivity={
            "_id": currActivityID,
            "modified_ts": modified_ts,
            "modified_by": modified_by,
            "changes": []
        }
        bugVersions.push(currActivity)
        bugVersionsMap[currActivityID]=currActivity
        prevActivityID=currActivityID
        currActivity.changes.push({
                "field_name": "attachment_added",
                "attach_id": attach_id
            })
    
    if not currBugAttachmentsMap[attach_id]:
        currBugAttachmentsMap[attach_id]={
            "attach_id": attach_id,
            "modified_ts": modified_ts,
            "modified_by": modified_by,
            "flags": []
        }
    
    currBugAttachmentsMap[attach_id][field_name]=field_value


def processFlagsTableItem(modified_ts, modified_by, field_name, field_value, field_value_removed, attach_id) :
    flag=makeFlag(field_value, modified_ts, modified_by)
    if attach_id != "":
        if not currBugAttachmentsMap[attach_id]:
            D.println("e", "Unable to find attachment " + attach_id + " for bug_id " + currBugID)
        
        currBugAttachmentsMap[attach_id].flags.push(flag)
    else:
        currBugState.flags.push(flag)
    


def processBugsActivitiesTableItem(modified_ts, modified_by, field_name, field_value, field_value_removed, attach_id) :
    if field_name == "flagtypes.name":
        field_name="flags"
    

    multi_field_value=getMultiFieldValue(field_name, field_value)
    multi_field_value_removed=getMultiFieldValue(field_name, field_value_removed)

    currActivityID=currBugID + "." + modified_ts
    if currActivityID != prevActivityID:
        currActivity=bugVersionsMap[currActivityID]
        if not currActivity:
            currActivity={
                "_id": currActivityID,
                "modified_ts": modified_ts,
                "modified_by": modified_by,
                "changes": []
            }
            bugVersions.push(currActivity)
        
        prevActivityID=currActivityID
    
    currActivity.changes.push({
        "field_name": field_name,
        "field_value": field_value,
        "field_value_removed": field_value_removed,
        "attach_id": attach_id
    })
    if attach_id != "":
        attachment=currBugAttachmentsMap[attach_id]
        if not attachment:
            D.println("e", "Unable to find attachment " + attach_id + " for bug_id "\
                + currBugID + ": " + CNV.object2JSON(currBugAttachmentsMap))
        else:
           if isinstance(attachment[field_name], list):
               a=attachment[field_name]
               #  Can have both added and removed values.
               if multi_field_value[0] != "":
                   removeValues(a, multi_field_value, "added", field_name, "attachment", attachment)
               

               if multi_field_value_removed[0] != "":
                   addValues(a, multi_field_value_removed, "removed attachment", field_name, currActivity)
               
           else:
               attachment[field_name]=field_value_removed
           
        
    else:
        if isinstance(currBugState[field_name], list):
            a=currBugState[field_name]
            #  Can have both added and removed values.
            if multi_field_value[0] != "":
                removeValues(a, multi_field_value, "added", field_name, "currBugState", currBugState)
            

            if multi_field_value_removed[0] != "":
                addValues(a, multi_field_value_removed, "removed bug", field_name, currActivity)
            
        elif isMultiField(field_name):
            #  field must currently be missing, otherwise it would
            #  be an instanceof Array above.  This handles multi-valued
            #  fields that are not first processed by processMultiValueTableItem().
            currBugState[field_name]=multi_field_value_removed
        else:
            #  Replace current value
            currBugState[field_name]=field_value_removed
        
    


def sortAscByField(a, b, aField) :
    if a[aField] > b[aField]:
        return 1
    if a[aField] < b[aField]:
        return -1
    return 0


def sortDescByField(a, b, aField) :
    return -1 * sortAscByField(a, b, aField)


def populateIntermediateVersionObjects() :
    #  Make sure the bugVersions are in descending order by modification time.
    #  They could be mixed because of attachment activity
    bugVersions.sort(lambda(a,b):sortDescByField(a, b, "modified_ts"))

    #  Tracks the previous distinct value for each field
    prevValues={}

    currVersion=None
    #  Prime the while loop with an empty next version so our first iteration outputs the initial bug state
    nextVersion={
        "_id":currBugState._id,
        "changes":[]
    }

    flagMap={}

    #  A monotonically increasing version number (useful for debugging)
    currBugVersion=1

    #  continue if there are more bug versions, or there is one final nextVersion
    while (len(bugVersions) > 0 or nextVersion) :
        currVersion=nextVersion
        if len(bugVersions) > 0:
          nextVersion=bugVersions.pop() #  Oldest version
        else:
          nextVersion=None
        
        D.println("Populating JSON for version " + currVersion._id)

        #  Link this version to the next one (if there is a next one)
        if nextVersion:
          D.println("We have a nextVersion:" + nextVersion.modified_ts
              + " (ver " + (currBugVersion + 1) + ")")
          currBugState.expires_on=nextVersion.modified_ts
        else:
          #  Otherwise, we don't know when the version expires.
          D.println("We have no nextVersion after #" + currBugVersion)
          currBugState.expires_on=None
        

        #  Copy all attributes from the current version into currBugState
        for propName in currVersion:
            currBugState[propName]=currVersion[propName]
        

        #  Now walk currBugState forward in time by applying the changes from currVersion
        changes=currVersion.changes
        # D.println("Processing changes: "+CNV.object2JSON(changes))
        for change in changes:
            D.println("Processing change: " + CNV.object2JSON(change))
            target=currBugState
            targetName="currBugState"
            attachID=change["attach_id"]
            if attachID != "":
                #  Handle the special change record that signals the creation of the attachment
                if change.field_name == "attachment_added":
                    #  This change only exists when the attachment has been added to the map, so no missing case needed.
                    currBugState.attachments.push(currBugAttachmentsMap[attachID])
                    continue
                else:
                    #  Attachment change
                    target=currBugAttachmentsMap[attachID]
                    targetName="attachment"
                    if target is None:
                        D.error("Encountered a change to missing attachment for bug '"
                              + currVersion["bug_id"] + "': " + CNV.object2JSON(change) + ".")

                        #  treat it as a change to the main bug instead :(
                        target=currBugState
                        targetName="currBugState"
                    
                
            

            #  Track the previous value
            if not isMultiField(change.field_name):
                #  Single-value field has changed in bug or attachment
                #  Make sure it's actually changing.  We seem to get change
                #   entries for attachments that show the current field value.
                if target[change.field_name] != change.field_value:
                   setPrevious(target, change.field_name, target[change.field_name], currVersion.modified_ts)
                else:
                   D.println("Skipping fake change to " + targetName + ": "\
                      + CNV.object2JSON(target) + ", change: " + CNV.object2JSON(change))
               
            elif change.field_name == "flags":
                processFlagChange(target, change, currVersion.modified_ts, currVersion.modified_by)
            else:
                D.println("Skipping previous_value for " + targetName + " multi-value field " + change.field_name)
            

            #  Multi-value fields
            if change.field_name == "flags":
                #  Already handled by "processFlagChange" above.
                D.println("Skipping previously processed flag change")
            elif isinstance(target[change.field_name], list):
                a=target[change.field_name]
                multi_field_value=getMultiFieldValue(change.field_name, change.field_value)
                multi_field_value_removed=getMultiFieldValue(change.field_name, change.field_value_removed)

                #  This was a deletion, find and delete the value(s)
                if multi_field_value_removed[0] != "":
                    removeValues(a, multi_field_value_removed, "removed", change.field_name, targetName, target)
                

                #  Handle addition(s) (if any)
                addValues(a, multi_field_value, "added", change.field_name, currVersion)
            elif isMultiField(change.field_name):
                #  First appearance of a multi-value field
                target[change.field_name]=[change.field_value]
            else:
                #  Simple field change.
                target[change.field_name]=change.field_value
            
        

        #  Do some processing to make sure that diffing betweens runs stays as similar as possible.
        stabilize(currBugState)

        #  Empty string breaks ES date parsing, remove it from bug state.
        for dateField in ["deadline", "cf_due_date"]:
            #  Special case to handle values that can't be parsed by ES:
            if currBugState[dateField] == "":
                #  Skip empty strings
                currBugState[dateField]=None
            elif currBugState[dateField] and currBugState[dateField].match(DATE_PATTERN):
                #  Convert "2012/01/01 00:00:00.000" to "2012-01-01"
                #  Example: bug 643420 (deadline)
                currBugState[dateField]=currBugState[dateField][0:10].replace("/", '-')

        

        currBugState.bug_version_num=currBugVersion
        currBugVersion+=1

    
    #  Output our wicked-sweet dupe lists:
    for dupe in dupeSingles:
        D.println("Found single dupe '" + dupe + "' " + dupeSingles[dupe] + " times.")
        newRow=Map()
        rowIndex=inputRowSize
        newRow[rowIndex++]=dupe
        newRow[rowIndex++]="single"
        newRow[rowIndex++]=dupeSingles[dupe]
        newRow[rowIndex++]=prevBugID
        putRow(newRow)
    
    dupeSingles={}

    for dupe in dupeMultis:
        D.println("Found multi dupe '" + dupe + "' " + dupeMultis[dupe] + " times.")
        newRow=Map()
        rowIndex=inputRowSize
        newRow[rowIndex++]=dupe
        newRow[rowIndex++]="multi"
        newRow[rowIndex++]=dupeMultis[dupe]
        newRow[rowIndex++]=prevBugID
        putRow(newRow)
    
    dupeMultis={}


def processFlagChange(aTarget, aChange, aTimestamp, aModifiedBy) :
    D.println("Processing flag change.  Added: '" + aChange.field_value
       + "', removed: '" + aChange.field_value_removed + "'")
    D.println("Target was: " + CNV.object2JSON(aTarget))
    addedFlags=getMultiFieldValue("flags", aChange.field_value)
    removedFlags=getMultiFieldValue("flags", aChange.field_value_removed)

    #  First, mark any removed flags as straight-up deletions.
    for flagStr in removedFlags:
        if flagStr == "":
            continue

        flag=makeFlag(flagStr, aTimestamp, aModifiedBy)
        existingFlag=findByKey(aTarget["flags"], "value", flagStr)

        if existingFlag:
            #  Carry forward some previous values:
            existingFlag["previous_modified_ts"]=existingFlag["modified_ts"]
            if existingFlag["modified_by"] != aModifiedBy:
                existingFlag["previous_modified_by"]=existingFlag["modified_by"]
                existingFlag["modified_by"]=aModifiedBy


            #  Add changed stuff:
            existingFlag["modified_ts"]=aTimestamp
            existingFlag["previous_status"]=flag["request_status"]
            existingFlag["previous_value"]=flagStr
            existingFlag["request_status"]="D"
            existingFlag["value"]=""
            #  request_type stays the same.
            #  requestee stays the same.

            duration_ms=existingFlag["modified_ts"] - existingFlag["previous_modified_ts"]
            existingFlag["duration_days"]= Math.floor(duration_ms / (1000.0 * 60 * 60 * 24))
        else:
            D.error("Did not find a corresponding flag for removed value '"+ flagStr + "' in " + CNV.object2JSON(aTarget["flags"]))
      
   

    #  See if we can align any of the added flags with previous deletions.
    #  If so, try to match them up with a "dangling" removed flag
    for flagStr in addedFlags:
        if flagStr == "":
            continue
      

        flag=makeFlag(flagStr, aTimestamp, aModifiedBy)

        if not aTarget["flags"]:
            D.println("Warning: processFlagChange called with unset 'flags'")
            aTarget["flags"]=[]
      

        candidates=aTarget["flags"].filter(lambda(element, index, array) :
            (element["value"] == ""
              and flag["request_type"] == element["request_type"]
              and flag["request_status"] != element["previous_status"]) #  Skip "r?(dre@mozilla)" -> "r?(mark@mozilla)"
        )

        if candidates:
            if len(candidates) >= 1:
                chosen_one=candidates[0]
                if len(candidates) > 1:
                    #  Multiple matches - use the best one.
                    D.println("Matched added flag " + CNV.object2JSON(flag) + " to multiple removed flags.  Using the best of these:")
                    for candidate in candidates:
                        D.println("      " + CNV.object2JSON(candidate))
               
                    matched_ts=candidates.filter(lambda(element, index, array) :
                        flag["modified_ts"] == element["modified_ts"]
                    )
                    if matched_ts and len(matched_ts) == 1:
                        D.println("Matching on modified_ts fixed it")
                        chosen_one=matched_ts[0]
                    else:
                        D.println("Matching on modified_ts left us with " +( len(matched_ts)  if matched_ts  else  "no")+ " matches")
                        #  If we had no matches (or many matches), try matching on requestee.
                        matched_req=candidates.filter(lambda(element, index, array) :
                            #  Do case-insenitive comparison
                            flag["modified_by"].toLowerCase() ==  element["requestee"].toLowerCase() if element["requestee"] else False
                        )
                        if matched_req and len(matched_req) == 1:
                            D.println("Matching on requestee fixed it")
                            chosen_one=matched_req[0]
                        else:
                            D.error("Matching on requestee left us with " +( len(matched_req)  if matched_req  else  "no")+ " matches. Skipping match.")
                     #  TODO: add "uncertain" flag?
                            chosen_one=None
                  
               
                else:
               #  Obvious case - matched exactly one.
                    D.println("Matched added flag " + CNV.object2JSON(flag) + " to removed flag " + CNV.object2JSON(chosen_one))
            

                if chosen_one:
                    for f in ["value", "request_status", "requestee"]:
                        if flag[f]:
                            chosen_one[f]=flag[f]
                  
               
            
            #  We need to avoid later adding this flag twice, since we rolled an add into a delete.
            else:
                #  No matching candidate. Totally new flag.
                D.println("Did not match added flag " + CNV.object2JSON(flag) + " to anything: " + CNV.object2JSON(aTarget["flags"]))
                aTarget["flags"].push(flag)
         
      
   


def setPrevious(dest, aFieldName, aValue, aChangeAway) :
    if not dest["previous_values"]:
       dest["previous_values"]={}
    

    pv=dest["previous_values"]
    vField= aFieldName + "_value"
    caField=aFieldName + "_change_away_ts"
    ctField=aFieldName + "_change_to_ts"
    ddField=aFieldName + "_duration_days"

    pv[vField]=aValue
    #  If we have a previous change for this field, then use the
    #  change-away time as the new change-to time.
    if pv[caField]:
       pv[ctField]=pv[caField]
    else:
       #  Otherwise, this is the first change for this field, so
       #  use the creation timestamp.
       pv[ctField]=dest["created_ts"]
    
    pv[caField]=aChangeAway
    duration_ms=pv[caField] - pv[ctField]
    pv[ddField]=Math.floor(duration_ms / (1000.0 * 60 * 60 * 24))


def findByKey(aList, aField, aValue) :
    for item in aList :
        if item[aField] == aValue:
            return item
      
   
    return None


def stabilize(aBug) :
    if aBug["cc"] and aBug["cc"][0]:
        aBug["cc"].sort()

    if aBug.changes is not None:
        Q.sort(aBug.changes, "field_name")



def makeFlag(flag, modified_ts, modified_by) :
    flagParts={
        "modified_ts": modified_ts,
        "modified_by": modified_by,
        "value": flag
   }
    matches=FLAG_PATTERN.exec(flag)
    if matches:
        flagParts.request_type=matches[1]
        flagParts.request_status=matches[2]
        if matches[3] and len(matches[3]) > 2:
            flagParts.requestee=matches[3].substring(1, len(matches[3]) - 1)
      
   
    return flagParts


def addValues(anArray, someValues, valueType, fieldName, anObj) :
    D.println("Adding " + valueType + " " + fieldName + " values:" + CNV.object2JSON(someValues))
    if fieldName == "flags":
        for added in someValues:
            if added != "":
                #  TODO: Some bugs (like 685605) actually have duplicate flags.  Do we want to keep them?
                #  Check if this flag has already been incorporated into a removed flag.  If so, don't add it again.

#                  dupes=anArray.filter(def(element, index, array) :
#                     return element["value"] == added
#                                and element["modified_by"] == anObj.modified_by
#                                and element["modified_ts"] == anObj.modified_ts
#                  })
#                  if dupes and len(dupes) > 0:
#                     D.println("Skipping duplicated added flag '" + added + "' since info is already in " + CNV.object2JSON(dupes[0]))
#                  else:

                 addedFlag=makeFlag(added, anObj.modified_ts, anObj.modified_by)
                 anArray.push(addedFlag)
    else:
        for added in someValues:
            if added != "":
                anArray.push(added)
          
      
   


def removeValues(anArray, someValues, valueType, fieldName, arrayDesc, anObj) :
    if fieldName == "flags":
        for v in someValues:
            length=len(anArray)
            for i in range(0, length):
                #  Match on flag name (incl. status) and flag value
                if anArray[i].value == v:
                    anArray.splice(i, 1)
                    break
                
            

            if length == len(anArray):
                D.error("Unable to find " + valueType + " flag " + fieldName + ":" + v
                                 + " in " + arrayDesc + ": " + CNV.object2JSON(anObj))

                dupeTarget=dupeSingles
                if len(anArray) > 1:
                    dupeTarget=dupeMultis
                

                vFlag=makeFlag(v, 0, 0)

                for item in anArray:
                    if vFlag.request_type == item.request_type and vFlag.request_status == item.request_status:
                        if dupeTarget[vFlag.requestee + "=" + item.requestee]:
                            dupeTarget[vFlag.requestee + "=" + item.requestee] += 1
                        else:
                            dupeTarget[vFlag.requestee + "=" + item.requestee]=1

                    else:
                          D.println("Skipping potential dupe: '" + v + "' != '" + item.value + "'")
    else:
        for v in someValues:
            foundAt=anArray.indexOf(v)
            if foundAt >= 0:
                anArray.splice(foundAt, 1)
            else:
                D.error("Unable to find " + valueType + " value " + fieldName + ":" + v + " in " + arrayDesc + ": " + CNV.object2JSON(anObj))
            
        
    


def isMultiField(aFieldName) :
    return (aFieldName == "flags" or aFieldName == "cc" or aFieldName == "keywords"
     or aFieldName == "dependson" or aFieldName == "blocked" or aFieldName == "dupe_by"
     or aFieldName == "dupe_of" or aFieldName == "bug_group")


def getMultiFieldValue(aFieldName, aFieldValue) :
    if isMultiField(aFieldName):
        return [v.trim() for v in aFieldValue.split(",")]
    return [aFieldValue]

