-- v12 (compatible with v8+): Add sender timestamp and prune index for privacy tokens
ALTER TABLE whatsmeow_privacy_tokens ADD sender_timestamp BIGINT;

CREATE INDEX idx_whatsmeow_privacy_tokens_our_jid_timestamp
ON whatsmeow_privacy_tokens (our_jid, timestamp_info);

ALTER TABLE whatsapp_message_node ADD CONSTRAINT [FK_whatsapp_message_node_our_jid] FOREIGN KEY ([our_jid]) REFERENCES whatsmeow_device ([jid]) ON DELETE CASCADE;