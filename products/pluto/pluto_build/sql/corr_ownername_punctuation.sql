-- Where the ownername has invalid leading punctuation, remove it and set dcpedited flag to true

UPDATE pluto a
SET ownername = trim(regexp_replace(a.ownername, '^([.,;><\-_!?`%]+)(.*)', '\2')),
    dcpedited= 't'
WHERE a.ownername ~ '^([.,;><\-_!?`%]+).*';
