# REMOVE UNEEDED RECORDS FROM DATABASE TO MAKE DATABASE FOR TESTING



use bugzilla;

DELETE FROM
	longdescs
WHERE
	bug_id not in {{bug_list}}
;

DELETE FROM	bugs_fulltext;


DELETE FROM
	bugs_activity
WHERE
	bug_id not in {{bug_list}}
;


DELETE FROM
	attachments
WHERE
	bug_id not in {{bug_list}}
;

DELETE FROM
	cc
WHERE
	bug_id not in {{bug_list}}
;

DELETE FROM
	flags
WHERE
	bug_id not in {{bug_list}}
;

DELETE FROM
	duplicates
WHERE
	dupe not in {{bug_list}}
	AND
	dupe_of not in {{bug_list}}

DELETE FROM
	dependencies
WHERE
	blocked not in {{bug_list}}
	AND
	dependson not in {{bug_list}}


DELETE FROM
	keywords
WHERE
	bug_id not in {{bug_list}}
;

DELETE FROM
	bug_see_also
WHERE
	bug_id not in {{bug_list}}
;

DELETE FROM
	bugs
WHERE
	bug_id not in {{bug_list}}
;

TRUNCATE series_data;
TRUNCATE email_setting;
TRUACATE bug_compare;
TRUNCATE attach_data;
TRUNCATE votes;
TRUNCATE bz_schema;
TRUNCATE profile_setting;
TRUNCATE series;
DROP TABLE cf_blocking_191;
DROP TABLE cf_blocking_192;
DROP TABLE cf_blocking_20;
DROP TABLE cf_blocking_b2g;
DROP TABLE cf_blocking_basecamp;
DROP TABLE cf_blocking_fennec;
DROP TABLE cf_blocking_fennec10;
DROP TABLE cf_blocking_fx;
DROP TABLE cf_blocking_kilimanjaro;
DROP TABLE cf_blocking_seamonkey21;
DROP TABLE cf_blocking_thunderbird30;
DROP TABLE cf_blocking_thunderbird31;
DROP TABLE cf_blocking_thunderbird32;
DROP TABLE cf_blocking_thunderbird33;
DROP TABLE cf_colo_site;
DROP TABLE cf_fixed_in;
DROP TABLE cf_locale;
DROP TABLE cf_office;
DROP TABLE cf_status192;
DROP TABLE cf_status_191;
DROP TABLE cf_status_192;
DROP TABLE cf_status_20;
DROP TABLE cf_status_b2g18;
DROP TABLE cf_status_b2g18_1_0_0;
DROP TABLE cf_status_b2g18_1_0_1;
DROP TABLE cf_status_b2g_1_1_hd;
DROP TABLE cf_status_esr10;
DROP TABLE cf_status_firefox10;
DROP TABLE cf_status_firefox11;
DROP TABLE cf_status_firefox12;
DROP TABLE cf_status_firefox13;
DROP TABLE cf_status_firefox14;
DROP TABLE cf_status_firefox15;
DROP TABLE cf_status_firefox16;
DROP TABLE cf_status_firefox17;
DROP TABLE cf_status_firefox18;
DROP TABLE cf_status_firefox19;
DROP TABLE cf_status_firefox20;
DROP TABLE cf_status_firefox21;
DROP TABLE cf_status_firefox22;
DROP TABLE cf_status_firefox23;
DROP TABLE cf_status_firefox24;
DROP TABLE cf_status_firefox25;
DROP TABLE cf_status_firefox5;
DROP TABLE cf_status_firefox6;
DROP TABLE cf_status_firefox7;
DROP TABLE cf_status_firefox8;
DROP TABLE cf_status_firefox9;
DROP TABLE cf_status_firefox_esr17;
DROP TABLE cf_status_seamonkey21;
DROP TABLE cf_status_seamonkey210;
DROP TABLE cf_status_seamonkey211;
DROP TABLE cf_status_seamonkey212;
DROP TABLE cf_status_seamonkey213;
DROP TABLE cf_status_seamonkey214;
DROP TABLE cf_status_seamonkey215;
DROP TABLE cf_status_seamonkey216;
DROP TABLE cf_status_seamonkey217;
DROP TABLE cf_status_seamonkey218;
DROP TABLE cf_status_seamonkey219;
DROP TABLE cf_status_seamonkey22;
DROP TABLE cf_status_seamonkey220;
DROP TABLE cf_status_seamonkey221;
DROP TABLE cf_status_seamonkey222;
DROP TABLE cf_status_seamonkey23;
DROP TABLE cf_status_seamonkey24;
DROP TABLE cf_status_seamonkey25;
DROP TABLE cf_status_seamonkey26;
DROP TABLE cf_status_seamonkey27;
DROP TABLE cf_status_seamonkey28;
DROP TABLE cf_status_seamonkey29;
DROP TABLE cf_status_thunderbird10;
DROP TABLE cf_status_thunderbird11;
DROP TABLE cf_status_thunderbird12;
DROP TABLE cf_status_thunderbird13;
DROP TABLE cf_status_thunderbird14;
DROP TABLE cf_status_thunderbird15;
DROP TABLE cf_status_thunderbird16;
DROP TABLE cf_status_thunderbird17;
DROP TABLE cf_status_thunderbird18;
DROP TABLE cf_status_thunderbird19;
DROP TABLE cf_status_thunderbird20;
DROP TABLE cf_status_thunderbird21;
DROP TABLE cf_status_thunderbird22;
DROP TABLE cf_status_thunderbird23;
DROP TABLE cf_status_thunderbird24;
DROP TABLE cf_status_thunderbird25;
DROP TABLE cf_status_thunderbird30;
DROP TABLE cf_status_thunderbird31;
DROP TABLE cf_status_thunderbird32;
DROP TABLE cf_status_thunderbird33;
DROP TABLE cf_status_thunderbird6;
DROP TABLE cf_status_thunderbird7;
DROP TABLE cf_status_thunderbird8;
DROP TABLE cf_status_thunderbird9;
DROP TABLE cf_status_thunderbird_esr10;
DROP TABLE cf_status_thunderbird_esr17;
DROP TABLE cf_tracking_b2g18;
DROP TABLE cf_tracking_esr10;
DROP TABLE cf_tracking_firefox10;
DROP TABLE cf_tracking_firefox11;
DROP TABLE cf_tracking_firefox12;
DROP TABLE cf_tracking_firefox13;
DROP TABLE cf_tracking_firefox14;
DROP TABLE cf_tracking_firefox15;
DROP TABLE cf_tracking_firefox16;
DROP TABLE cf_tracking_firefox17;
DROP TABLE cf_tracking_firefox18;
DROP TABLE cf_tracking_firefox19;
DROP TABLE cf_tracking_firefox20;
DROP TABLE cf_tracking_firefox21;
DROP TABLE cf_tracking_firefox22;
DROP TABLE cf_tracking_firefox23;
DROP TABLE cf_tracking_firefox24;
DROP TABLE cf_tracking_firefox25;
DROP TABLE cf_tracking_firefox5;
DROP TABLE cf_tracking_firefox6;
DROP TABLE cf_tracking_firefox7;
DROP TABLE cf_tracking_firefox8;
DROP TABLE cf_tracking_firefox9;
DROP TABLE cf_tracking_firefox_esr17;
DROP TABLE cf_tracking_firefox_relnote;
DROP TABLE cf_tracking_relnote_b2g;
DROP TABLE cf_tracking_seamonkey210;
DROP TABLE cf_tracking_seamonkey211;
DROP TABLE cf_tracking_seamonkey212;
DROP TABLE cf_tracking_seamonkey213;
DROP TABLE cf_tracking_seamonkey214;
DROP TABLE cf_tracking_seamonkey215;
DROP TABLE cf_tracking_seamonkey216;
DROP TABLE cf_tracking_seamonkey217;
DROP TABLE cf_tracking_seamonkey218;
DROP TABLE cf_tracking_seamonkey219;
DROP TABLE cf_tracking_seamonkey22;
DROP TABLE cf_tracking_seamonkey220;
DROP TABLE cf_tracking_seamonkey221;
DROP TABLE cf_tracking_seamonkey222;
DROP TABLE cf_tracking_seamonkey23;
DROP TABLE cf_tracking_seamonkey24;
DROP TABLE cf_tracking_seamonkey25;
DROP TABLE cf_tracking_seamonkey26;
DROP TABLE cf_tracking_seamonkey27;
DROP TABLE cf_tracking_seamonkey28;
DROP TABLE cf_tracking_seamonkey29;
DROP TABLE cf_tracking_thunderbird10;
DROP TABLE cf_tracking_thunderbird11;
DROP TABLE cf_tracking_thunderbird12;
DROP TABLE cf_tracking_thunderbird13;
DROP TABLE cf_tracking_thunderbird14;
DROP TABLE cf_tracking_thunderbird15;
DROP TABLE cf_tracking_thunderbird16;
DROP TABLE cf_tracking_thunderbird17;
DROP TABLE cf_tracking_thunderbird18;
DROP TABLE cf_tracking_thunderbird19;
DROP TABLE cf_tracking_thunderbird20;
DROP TABLE cf_tracking_thunderbird21;
DROP TABLE cf_tracking_thunderbird22;
DROP TABLE cf_tracking_thunderbird23;
DROP TABLE cf_tracking_thunderbird24;
DROP TABLE cf_tracking_thunderbird25;
DROP TABLE cf_tracking_thunderbird6;
DROP TABLE cf_tracking_thunderbird7;
DROP TABLE cf_tracking_thunderbird8;
DROP TABLE cf_tracking_thunderbird9;
DROP TABLE cf_tracking_thunderbird_esr10;
DROP TABLE cf_tracking_thunderbird_esr17;



