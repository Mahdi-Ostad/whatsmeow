-- v15 (compatible with v8+): Add companion meta nonce column to device table
ALTER TABLE whatsmeow_device ADD companion_meta_nonce NVARCHAR(MAX) NOT NULL DEFAULT '';
