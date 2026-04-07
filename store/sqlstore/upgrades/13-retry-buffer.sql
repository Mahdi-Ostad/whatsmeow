-- v13 (compatible with v8+): Add buffer for outgoing events to accept retry receipts
CREATE TABLE whatsmeow_retry_buffer (
	our_jid    VARCHAR(300)   NOT NULL,
	chat_jid   VARCHAR(300)   NOT NULL,
	message_id VARCHAR(200)   NOT NULL,
	format     VARCHAR(300)   NOT NULL,
	plaintext  VARBINARY(MAX)  NOT NULL,
	timestamp_info  BIGINT NOT NULL,

	PRIMARY KEY (our_jid, chat_jid, message_id),
	FOREIGN KEY (our_jid) REFERENCES whatsmeow_device(jid) ON DELETE CASCADE ON UPDATE CASCADE
);

CREATE INDEX whatsmeow_retry_buffer_timestamp_idx ON whatsmeow_retry_buffer (our_jid, timestamp_info);
