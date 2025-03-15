// Copyright (c) 2021 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package sqlstore

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
)

type upgradeFunc func(*sql.Tx, *Container) error

// Upgrades is a list of functions that will upgrade a database to the latest version.
//
// This may be of use if you want to manage the database fully manually, but in most cases you
// should just call Container.Upgrade to let the library handle everything.
var Upgrades = [...]upgradeFunc{upgradeV1, upgradeV2, upgradeV3, upgradeV4, upgradeV5, upgradeV6}

func (c *Container) getVersion() (int, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	cQuery := "CREATE TABLE IF NOT EXISTS whatsmeow_version (version INTEGER)"
	if c.dialect == "sqlserver" {
		cQuery = `
		IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'whatsmeow_version')
		BEGIN
			CREATE TABLE whatsmeow_version (version_info INTEGER)
		END;`
	}
	_, err := c.db.Exec(cQuery)
	if err != nil {
		return -1, err
	}

	version := 0
	vQuery := "SELECT version FROM whatsmeow_version LIMIT 1"
	if c.dialect == "sqlserver" {
		vQuery = "SELECT TOP 1 version_info FROM whatsmeow_version"
	}
	row := c.db.QueryRow(vQuery)
	if row == nil {
		return -1, fmt.Errorf("select rows of whatsmeow_version table has problem.")
	}
	err = row.Scan(&version)
	if err != nil && err.Error() != "sql: no rows in result set" {
		return -1, err
	}
	return version, nil
}

func (c *Container) setVersion(tx *sql.Tx, version int) error {
	_, err := tx.Exec("DELETE FROM whatsmeow_version")
	if err != nil {
		return err
	}
	if c.dialect == "sqlserver" {
		_, err = tx.Exec(fmt.Sprintf("INSERT INTO whatsmeow_version (version_info) VALUES (%s)", strconv.Itoa(version)))
		return err
	}
	_, err = tx.Exec(fmt.Sprintf("INSERT INTO whatsmeow_version (version) VALUES (%s)", strconv.Itoa(version)))
	return err
}

// Upgrade upgrades the database from the current to the latest version available.
func (c *Container) Upgrade() error {
	if c.dialect == "sqlite3" {
		var foreignKeysEnabled bool
		err := c.db.QueryRow("PRAGMA foreign_keys").Scan(&foreignKeysEnabled)
		if err != nil {
			return fmt.Errorf("failed to check if foreign keys are enabled: %w", err)
		} else if !foreignKeysEnabled {
			return fmt.Errorf("foreign keys are not enabled")
		}
	}

	version, err := c.getVersion()
	if err != nil {
		return err
	}

	for ; version < len(Upgrades); version++ {
		var tx *sql.Tx
		tx, err = c.db.Begin()
		if err != nil {
			return err
		}

		migrateFunc := Upgrades[version]
		c.log.Infof("Upgrading database to v%d", version+1)
		err = migrateFunc(tx, c)
		if err != nil {
			_ = tx.Rollback()
			return err
		}

		if err = c.setVersion(tx, version+1); err != nil {
			return err
		}

		if err = tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}

func getWhatsmeowDeviceCreate(dialect string) string {
	if dialect == "sqlserver" {
		return `CREATE TABLE whatsmeow_device (
			jid VARCHAR(300) PRIMARY KEY ,
			registration_id BIGINT NOT NULL CHECK ( registration_id >= 0 AND registration_id < 4294967296 ),
			noise_key    VARBINARY(max) NOT NULL CHECK ( LEN(noise_key) = 32 ),
			identity_key VARBINARY(max) NOT NULL CHECK ( LEN(identity_key) = 32 ),
			signed_pre_key     VARBINARY(max)   NOT NULL CHECK ( LEN(signed_pre_key) = 32 ),
			signed_pre_key_id  INTEGER NOT NULL CHECK ( signed_pre_key_id >= 0 AND signed_pre_key_id < 16777216 ),
			signed_pre_key_sig VARBINARY(max)   NOT NULL CHECK ( LEN(signed_pre_key_sig) = 64 ),
			adv_key         VARBINARY(max) NOT NULL,
			adv_details     VARBINARY(max) NOT NULL,
			adv_account_sig VARBINARY(max) NOT NULL CHECK ( LEN(adv_account_sig) = 64 ),
			adv_device_sig  VARBINARY(max) NOT NULL CHECK ( LEN(adv_device_sig) = 64 ),
			platform      VARCHAR(300) NOT NULL DEFAULT '',
			business_name VARCHAR(300) NOT NULL DEFAULT '',
			push_name     VARCHAR(300) NOT NULL DEFAULT '',
			manager_id     VARCHAR(300) NOT NULL DEFAULT ''
		);`
	}
	return `CREATE TABLE whatsmeow_device (
		jid TEXT PRIMARY KEY,

		registration_id BIGINT NOT NULL CHECK ( registration_id >= 0 AND registration_id < 4294967296 ),

		noise_key    bytea NOT NULL CHECK ( length(noise_key) = 32 ),
		identity_key bytea NOT NULL CHECK ( length(identity_key) = 32 ),

		signed_pre_key     bytea   NOT NULL CHECK ( length(signed_pre_key) = 32 ),
		signed_pre_key_id  INTEGER NOT NULL CHECK ( signed_pre_key_id >= 0 AND signed_pre_key_id < 16777216 ),
		signed_pre_key_sig bytea   NOT NULL CHECK ( length(signed_pre_key_sig) = 64 ),

		adv_key         bytea NOT NULL,
		adv_details     bytea NOT NULL,
		adv_account_sig bytea NOT NULL CHECK ( length(adv_account_sig) = 64 ),
		adv_device_sig  bytea NOT NULL CHECK ( length(adv_device_sig) = 64 ),
		
		platform      TEXT NOT NULL DEFAULT '',
		business_name TEXT NOT NULL DEFAULT '',
		push_name     TEXT NOT NULL DEFAULT '',
		manager_id     TEXT NOT NULL DEFAULT ''
	)`
}

func getWhatsmeowIdentityKeys(dialect string) string {
	if dialect == "sqlserver" {
		return `CREATE TABLE whatsmeow_identity_keys (
			our_jid  VARCHAR(300),
			their_id VARCHAR(300),
			identity_info VARBINARY(max) NOT NULL CHECK ( LEN(identity_info) = 32 ),
			PRIMARY KEY (our_jid, their_id),
			FOREIGN KEY (our_jid) REFERENCES whatsmeow_device(jid) ON DELETE CASCADE ON UPDATE CASCADE
		)`
	}
	return `CREATE TABLE whatsmeow_identity_keys (
		our_jid  TEXT,
		their_id TEXT,
		identity bytea NOT NULL CHECK ( length(identity) = 32 ),

		PRIMARY KEY (our_jid, their_id),
		FOREIGN KEY (our_jid) REFERENCES whatsmeow_device(jid) ON DELETE CASCADE ON UPDATE CASCADE
	)`
}

func getWhatsmeowPreKeys(dialect string) string {
	// TODO: be carefull about BIT and BOOLEAN and insert
	if dialect == "sqlserver" {
		return `CREATE TABLE whatsmeow_pre_keys (
			jid      VARCHAR(300),
			key_id   INTEGER  CHECK ( key_id >= 0 AND key_id < 16777216 ),
			key_info       VARBINARY(max)  NOT NULL CHECK (LEN(key_info) = 32 ),
			uploaded BIT NOT NULL,
	
			PRIMARY KEY (jid, key_id),
			FOREIGN KEY (jid) REFERENCES whatsmeow_device(jid) ON DELETE CASCADE ON UPDATE CASCADE
		)`
	}
	return `CREATE TABLE whatsmeow_pre_keys (
		jid      TEXT,
		key_id   INTEGER          CHECK ( key_id >= 0 AND key_id < 16777216 ),
		key      bytea   NOT NULL CHECK ( length(key) = 32 ),
		uploaded BOOLEAN NOT NULL,

		PRIMARY KEY (jid, key_id),
		FOREIGN KEY (jid) REFERENCES whatsmeow_device(jid) ON DELETE CASCADE ON UPDATE CASCADE
	)`
}
func getWhatsmeowSessions(dialect string) string {
	if dialect == "sqlserver" {
		return `CREATE TABLE whatsmeow_sessions (
			our_jid   VARCHAR(300),
			their_id  VARCHAR(300),
			session  VARBINARY(max),
	
			PRIMARY KEY (our_jid, their_id),
			FOREIGN KEY (our_jid) REFERENCES whatsmeow_device(jid) ON DELETE CASCADE ON UPDATE CASCADE
		)`
	}
	return `CREATE TABLE whatsmeow_sessions (
		our_jid  TEXT,
		their_id TEXT,
		session  bytea,

		PRIMARY KEY (our_jid, their_id),
		FOREIGN KEY (our_jid) REFERENCES whatsmeow_device(jid) ON DELETE CASCADE ON UPDATE CASCADE
	)`
}

func getWhatsmeowSenderKeys(dialect string) string {
	if dialect == "sqlserver" {
		return `	CREATE TABLE whatsmeow_sender_keys (
			our_jid     VARCHAR(300),
			chat_id     VARCHAR(300),
			sender_id  VARCHAR(300),
			sender_key  VARBINARY(max) NOT NULL,
	
			PRIMARY KEY (our_jid, chat_id, sender_id),
			FOREIGN KEY (our_jid) REFERENCES whatsmeow_device(jid) ON DELETE CASCADE ON UPDATE CASCADE
		)`
	}
	return `CREATE TABLE whatsmeow_sender_keys (
		our_jid    TEXT,
		chat_id    TEXT,
		sender_id  TEXT,
		sender_key bytea NOT NULL,

		PRIMARY KEY (our_jid, chat_id, sender_id),
		FOREIGN KEY (our_jid) REFERENCES whatsmeow_device(jid) ON DELETE CASCADE ON UPDATE CASCADE
	)`
}

func getAppStateSyncKeys(dialect string) string {
	if dialect == "sqlserver" {
		return `CREATE TABLE whatsmeow_app_state_sync_keys (
			jid         VARCHAR(300),
			key_id      VARBINARY(300),
			key_data    VARBINARY(max)  NOT NULL,
			timestamp_info   BIGINT NOT NULL,
			fingerprint VARBINARY(max)  NOT NULL,
	
			PRIMARY KEY (jid, key_id),
			FOREIGN KEY (jid) REFERENCES whatsmeow_device(jid) ON DELETE CASCADE ON UPDATE CASCADE
		)`
	}
	return `CREATE TABLE whatsmeow_app_state_sync_keys (
		jid         TEXT,
		key_id      bytea,
		key_data    bytea  NOT NULL,
		timestamp   BIGINT NOT NULL,
		fingerprint bytea  NOT NULL,

		PRIMARY KEY (jid, key_id),
		FOREIGN KEY (jid) REFERENCES whatsmeow_device(jid) ON DELETE CASCADE ON UPDATE CASCADE
	)`
}

func getWhatsmeowAppStateVersion(dialect string) string {
	if dialect == "sqlserver" {
		return `CREATE TABLE whatsmeow_app_state_version (
			jid     VARCHAR(300),
			name    VARCHAR(300),
			version_info BIGINT NOT NULL,
			hash    VARBINARY(max)  NOT NULL CHECK ( LEN(hash) = 128 ),
	
			PRIMARY KEY (jid, name),
			FOREIGN KEY (jid) REFERENCES whatsmeow_device(jid) ON DELETE CASCADE ON UPDATE CASCADE
		)`
	}

	return `CREATE TABLE whatsmeow_app_state_version (
		jid     TEXT,
		name    TEXT,
		version BIGINT NOT NULL,
		hash    bytea  NOT NULL CHECK ( length(hash) = 128 ),

		PRIMARY KEY (jid, name),
		FOREIGN KEY (jid) REFERENCES whatsmeow_device(jid) ON DELETE CASCADE ON UPDATE CASCADE
	)`
}

func getWhatsappAppStateMutationMacs(dialect string) string {
	if dialect == "sqlserver" {
		return `CREATE TABLE whatsmeow_app_state_mutation_macs (
			jid        VARCHAR(300),
			name       VARCHAR(300),
			version_info   BIGINT,
			index_mac VARBINARY(200) CHECK ( LEN(index_mac) = 32 ),
			value_mac VARBINARY(max) NOT NULL CHECK ( LEN(value_mac) = 32 ),
	
			PRIMARY KEY (jid, name, version_info, index_mac),
			FOREIGN KEY (jid, name) REFERENCES whatsmeow_app_state_version(jid, name) ON DELETE CASCADE ON UPDATE CASCADE
		)`
	}
	return `CREATE TABLE whatsmeow_app_state_mutation_macs (
		jid       TEXT,
		name      TEXT,
		version   BIGINT,
		index_mac bytea          CHECK ( length(index_mac) = 32 ),
		value_mac bytea NOT NULL CHECK ( length(value_mac) = 32 ),

		PRIMARY KEY (jid, name, version, index_mac),
		FOREIGN KEY (jid, name) REFERENCES whatsmeow_app_state_version(jid, name) ON DELETE CASCADE ON UPDATE CASCADE
	)`
}

func getWhatsappiumContacts(dialect string) string {
	if dialect == "sqlserver" {
		return `CREATE TABLE whatsmeow_contacts (
			our_jid       VARCHAR(300),
			their_jid     VARCHAR(300),
			first_name    VARCHAR(300),
			full_name     VARCHAR(300),
			push_name     VARCHAR(300),
			business_name VARCHAR(300),
	
			PRIMARY KEY (our_jid, their_jid),
			FOREIGN KEY (our_jid) REFERENCES whatsmeow_device(jid) ON DELETE CASCADE ON UPDATE CASCADE
		)`
	}
	return `CREATE TABLE whatsmeow_contacts (
		our_jid       TEXT,
		their_jid     TEXT,
		first_name    TEXT,
		full_name     TEXT,
		push_name     TEXT,
		business_name TEXT,

		PRIMARY KEY (our_jid, their_jid),
		FOREIGN KEY (our_jid) REFERENCES whatsmeow_device(jid) ON DELETE CASCADE ON UPDATE CASCADE
	)`
}

func getWhatsappChatSettings(dialect string) string {
	if dialect == "sqlserver" {
		return `CREATE TABLE whatsmeow_chat_settings (
			our_jid       VARCHAR(300),
			chat_jid      VARCHAR(300),
			muted_until   BIGINT  NOT NULL DEFAULT 0,
			pinned        BIT NOT NULL DEFAULT 0,
			archived      BIT NOT NULL DEFAULT 1,
	
			PRIMARY KEY (our_jid, chat_jid),
			FOREIGN KEY (our_jid) REFERENCES whatsmeow_device(jid) ON DELETE CASCADE ON UPDATE CASCADE
		)`
	}
	return `CREATE TABLE whatsmeow_chat_settings (
		our_jid       TEXT,
		chat_jid      TEXT,
		muted_until   BIGINT  NOT NULL DEFAULT 0,
		pinned        BOOLEAN NOT NULL DEFAULT false,
		archived      BOOLEAN NOT NULL DEFAULT false,

		PRIMARY KEY (our_jid, chat_jid),
		FOREIGN KEY (our_jid) REFERENCES whatsmeow_device(jid) ON DELETE CASCADE ON UPDATE CASCADE
	)`
}

func upgradeV1(tx *sql.Tx, c *Container) error {
	_, err := tx.Exec(getWhatsmeowDeviceCreate(c.dialect))
	if err != nil {
		return err
	}
	_, err = tx.Exec(getWhatsmeowIdentityKeys(c.dialect))
	if err != nil {
		return err
	}
	_, err = tx.Exec(getWhatsmeowPreKeys(c.dialect))
	if err != nil {
		return err
	}
	_, err = tx.Exec(getWhatsmeowSessions(c.dialect))
	if err != nil {
		return err
	}
	_, err = tx.Exec(getWhatsmeowSenderKeys(c.dialect))
	if err != nil {
		return err
	}
	_, err = tx.Exec(getAppStateSyncKeys(c.dialect))
	if err != nil {
		return err
	}
	_, err = tx.Exec(getWhatsmeowAppStateVersion(c.dialect))
	if err != nil {
		return err
	}
	_, err = tx.Exec(getWhatsappAppStateMutationMacs(c.dialect))
	if err != nil {
		return err
	}
	_, err = tx.Exec(getWhatsappiumContacts(c.dialect))
	if err != nil {
		return err
	}
	_, err = tx.Exec(getWhatsappChatSettings(c.dialect))
	if err != nil {
		return err
	}
	return nil
}

const fillSigKeyPostgres = `
UPDATE whatsmeow_device SET adv_account_sig_key=(
	SELECT identity
	FROM whatsmeow_identity_keys
	WHERE our_jid=whatsmeow_device.jid
	  AND their_id=concat(split_part(whatsmeow_device.jid, '.', 1), ':0')
);
DELETE FROM whatsmeow_device WHERE adv_account_sig_key IS NULL;
ALTER TABLE whatsmeow_device ALTER COLUMN adv_account_sig_key SET NOT NULL;
`

const fillSigKeySQLite = `
UPDATE whatsmeow_device SET adv_account_sig_key=(
	SELECT identity
	FROM whatsmeow_identity_keys
	WHERE our_jid=whatsmeow_device.jid
	  AND their_id=substr(whatsmeow_device.jid, 0, instr(whatsmeow_device.jid, '.')) || ':0'
)
`

const fillSigKeySqlServer = `
UPDATE whatsmeow_device 
SET adv_account_sig = (
    SELECT identity_info
    FROM whatsmeow_identity_keys
    WHERE our_jid = whatsmeow_device.jid
    AND their_id = SUBSTRING(whatsmeow_device.jid, 0, CHARINDEX('.', whatsmeow_device.jid)) + ':0'
)
`

func upgradeV2(tx *sql.Tx, container *Container) error {
	cmd := "ALTER TABLE whatsmeow_device ADD COLUMN adv_account_sig_key bytea CHECK ( length(adv_account_sig_key) = 32 )"
	if container.dialect == "sqlserver" {
		cmd = "ALTER TABLE whatsmeow_device ADD adv_account_sig_key VARBINARY(max) CHECK (LEN(adv_account_sig_key) = 32)"
	}
	_, err := tx.Exec(cmd)
	if err != nil {
		return err
	}

	if strings.Contains(container.dialect, "postgres") || container.dialect == "pgx" {
		_, err = tx.Exec(fillSigKeyPostgres)
	} else if container.dialect == "sqlserver" {
		_, err = tx.Exec(fillSigKeySqlServer)
	} else {
		_, err = tx.Exec(fillSigKeySQLite)
	}
	return err
}

func upgradeV3(tx *sql.Tx, container *Container) error {
	cmd :=
		`CREATE TABLE whatsmeow_message_secrets (
		our_jid    TEXT,
		chat_jid   TEXT,
		sender_jid TEXT,
		message_id TEXT,
		key        bytea NOT NULL,

		PRIMARY KEY (our_jid, chat_jid, sender_jid, message_id),
		FOREIGN KEY (our_jid) REFERENCES whatsmeow_device(jid) ON DELETE CASCADE ON UPDATE CASCADE
	)`
	if container.dialect == "sqlserver" {
		cmd = `CREATE TABLE whatsmeow_message_secrets (
			our_jid    VARCHAR(300),
			chat_jid   VARCHAR(200),
			sender_jid  VARCHAR(200),
			message_id VARCHAR(200),
			key_info  VARBINARY(max) NOT NULL,
	
			PRIMARY KEY (our_jid, chat_jid, sender_jid, message_id),
			FOREIGN KEY (our_jid) REFERENCES whatsmeow_device(jid) ON DELETE CASCADE ON UPDATE CASCADE
		)
	`
	}
	_, err := tx.Exec(cmd)
	return err
}

func upgradeV4(tx *sql.Tx, container *Container) error {
	cmd := `CREATE TABLE whatsmeow_privacy_tokens (
		our_jid   TEXT,
		their_jid TEXT,
		token     bytea  NOT NULL,
		timestamp BIGINT NOT NULL,
		PRIMARY KEY (our_jid, their_jid)
	)`
	if container.dialect == "sqlserver" {
		cmd = `	CREATE TABLE whatsmeow_privacy_tokens (
			our_jid   VARCHAR(300),
			their_jid VARCHAR(300),
			token     VARBINARY(max)  NOT NULL,
			timestamp_info BIGINT NOT NULL,
			PRIMARY KEY (our_jid, their_jid)
		)`
	}
	_, err := tx.Exec(cmd)
	return err
}

func upgradeV5(tx *sql.Tx, container *Container) error {
	_, err := tx.Exec("UPDATE whatsmeow_device SET jid=REPLACE(jid, '.0', '')")
	return err
}

func upgradeV6(tx *sql.Tx, container *Container) error {
	_, err := tx.Exec("ALTER TABLE whatsmeow_device ADD facebook_uuid uniqueidentifier NULL")
	return err
}
