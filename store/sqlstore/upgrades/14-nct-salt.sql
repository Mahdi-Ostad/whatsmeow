-- v14 (compatible with v8+): Add NCT salt table for cstoken derivation
CREATE TABLE whatsmeow_nct_salt (
	our_jid VARCHAR(300) PRIMARY KEY,
	salt    VARBINARY(max) NOT NULL,
	FOREIGN KEY (our_jid) REFERENCES whatsmeow_device(jid) ON DELETE CASCADE ON UPDATE CASCADE
);
