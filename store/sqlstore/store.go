// Copyright (c) 2022 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

// Package sqlstore contains an SQL-backed implementation of the interfaces in the store package.
package sqlstore

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/util/keys"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// ErrInvalidLength is returned by some database getters if the database returned a byte array with an unexpected length.
// This should be impossible, as the database schema contains CHECK()s for all the relevant columns.
var ErrInvalidLength = errors.New("database returned byte array with illegal length")

// PostgresArrayWrapper is a function to wrap array values before passing them to the sql package.
//
// When using github.com/lib/pq, you should set
//
//	whatsmeow.PostgresArrayWrapper = pq.Array
var PostgresArrayWrapper func(interface{}) interface {
	driver.Valuer
	sql.Scanner
}

type SQLStore struct {
	*Container
	JID string

	preKeyLock sync.Mutex

	contactCache     map[types.JID]*types.ContactInfo
	contactCacheLock sync.Mutex
}

type contactUpdate struct {
	sqlStore *SQLStore
	contacts []store.ContactEntry
}

type senderkeyUpdate struct {
	sqlStore *SQLStore
	group    string
	user     string
	session  []byte
	err      chan<- error
}

type sessionUpdate struct {
	address string
	session []byte
	jid     string
}

type identityUpdate struct {
	address string
	key     [32]byte
	jid     string
}

var sqlInstance *sql.DB
var contactsChannel = make(chan contactUpdate)
var senderkeysChannel = make(chan senderkeyUpdate)
var sessionsChannel = make(chan sessionUpdate)
var identitiesChannel = make(chan identityUpdate)

var identityList = []identityUpdate{}
var sessionList = []sessionUpdate{}

// NewSQLStore creates a new SQLStore with the given database container and user JID.
// It contains implementations of all the different stores in the store package.
//
// In general, you should use Container.NewDevice or Container.GetDevice instead of this.
func NewSQLStore(c *Container, jid types.JID) *SQLStore {
	if sqlInstance == nil {
		sqlInstance = c.db
	}
	return &SQLStore{
		Container:    c,
		JID:          jid.String(),
		contactCache: make(map[types.JID]*types.ContactInfo),
	}
}

func ManageContacts(logger waLog.Logger) {
	defer func() {
		if value := recover(); value != nil {
			err, ok := value.(error)
			if ok {
				logger.Fatalf(err.Error())
			} else {
				logger.Fatalf("Manage Contacts Recovered:" + err.Error())
			}
		}
	}()
	for contactUpdate := range contactsChannel {
		if len(contactUpdate.contacts) > contactBatchSize {
			tx, err := sqlInstance.Begin()
			if err != nil {
				logger.Errorf("failed to start transaction: " + err.Error())
				continue
			}
			for i := 0; i < len(contactUpdate.contacts); i += contactBatchSize {
				var contactSlice []store.ContactEntry
				if len(contactUpdate.contacts) > i+contactBatchSize {
					contactSlice = contactUpdate.contacts[i : i+contactBatchSize]
				} else {
					contactSlice = contactUpdate.contacts[i:]
				}
				err = contactUpdate.sqlStore.putContactNamesBatch(tx, contactSlice)
				if err != nil {
					break
				}
			}
			if err != nil {
				logger.Errorf(err.Error())
				tx.Rollback()
				continue
			}
			err = tx.Commit()
			if err != nil {
				logger.Errorf("failed to commit transaction: " + err.Error())
				continue
			}
		} else if len(contactUpdate.contacts) > 0 {
			err := contactUpdate.sqlStore.putContactNamesBatch(contactUpdate.sqlStore.db, contactUpdate.contacts)
			if err != nil {
				logger.Errorf(err.Error())
				continue
			}
		} else {
			continue
		}
		contactUpdate.sqlStore.contactCacheLock.Lock()
		// Just clear the cache, fetching pushnames and business names would be too much effort
		// I'll do it myself then F U XD
		for _, updates := range contactUpdate.contacts {
			delete(contactUpdate.sqlStore.contactCache, updates.JID)
		}
		contactUpdate.sqlStore.contactCacheLock.Unlock()
	}
}

func ManageSenderKeys() {
	for update := range senderkeysChannel {
		err := manageSingleSenderKey(update)
		update.err <- err
	}
}

func ManageIdentities(logger waLog.Logger) {
	go identityUpdateBackgroundService(logger)
	for update := range identitiesChannel {
		identityList = append(identityList, update)
	}
}

func identityUpdateBackgroundService(logger waLog.Logger) {
	for {
		time.Sleep(time.Minute * 1)
		if len(identityList) == 0 {
			logger.Infof("No Identity to Update")
			continue
		}
		logger.Infof("Storing Identities Starting")
		identities := make([]identityUpdate, len(identityList))
		copy(identities, identityList)
		identityList = []identityUpdate{}
		for _, update := range identities {
			err := manageSingleIdentity(update)
			if err != nil {
				logger.Errorf("Could not store identitykey: JID: " + update.jid + ", AddressId: " + update.address + ";Error is: " + err.Error())
				if !strings.Contains(err.Error(), "KEY constraint") {
					identitiesChannel <- update
				}
			}
		}
		logger.Infof("Storing Identities Complete")
	}
}

func manageSingleIdentity(identityUpdate identityUpdate) error {
	// sqlInstance.mutex.Lock()
	// defer identityUpdate.sqlStore.mutex.Unlock()
	// if identityUpdate.sqlStore.dialect == "sqlserver" {
	// 	_, err := identityUpdate.sqlStore.db.Exec(mssqlPutIdentityQuery, identityUpdate.sqlStore.JID, identityUpdate.address, identityUpdate.key[:])
	// 	return err
	// }
	// _, err := identityUpdate.sqlStore.db.Exec(sqlitePutIdentityQuery, identityUpdate.sqlStore.JID, identityUpdate.address, identityUpdate.key[:])
	// return err
	_, err := sqlInstance.Exec(mssqlPutIdentityQuery, identityUpdate.jid, identityUpdate.address, identityUpdate.key[:])
	return err
}

func manageSingleSenderKey(senderkeyUpdate senderkeyUpdate) error {
	senderkeyUpdate.sqlStore.mutex.Lock()
	defer senderkeyUpdate.sqlStore.mutex.Unlock()
	if senderkeyUpdate.sqlStore.dialect == "sqlserver" {
		_, err := senderkeyUpdate.sqlStore.db.Exec(mssqlPutSenderKeyQuery, senderkeyUpdate.sqlStore.JID, senderkeyUpdate.group, senderkeyUpdate.user, senderkeyUpdate.session)
		return err
	}
	_, err := senderkeyUpdate.sqlStore.db.Exec(sqlitePutSenderKeyQuery, senderkeyUpdate.sqlStore.JID, senderkeyUpdate.group, senderkeyUpdate.user, senderkeyUpdate.session)
	return err
}

func ManageSessions(logger waLog.Logger) {
	go sessionUpdateBackgroundService(logger)
	for update := range sessionsChannel {
		sessionList = append(sessionList, update)
	}
}

func sessionUpdateBackgroundService(logger waLog.Logger) {
	for {
		time.Sleep(time.Minute * 1)
		if len(sessionList) == 0 {
			logger.Infof("No Session to Update")
			continue
		}
		logger.Infof("Storing Sessions Starting")
		sessions := make([]sessionUpdate, len(sessionList))
		copy(sessions, sessionList)
		sessionList = []sessionUpdate{}
		for _, update := range sessions {
			err := manageSingleSession(update)
			if err != nil {
				logger.Errorf("Could not store session: JID: " + update.jid + ", AddressId: " + update.address + ";Error is: " + err.Error())
				if !strings.Contains(err.Error(), "KEY constraint") {
					sessionsChannel <- update
				}
			}
		}
		logger.Infof("Storing Sessions Complete")
	}
}

func manageSingleSession(sessionUpdate sessionUpdate) error {
	// sessionUpdate.sqlStore.mutex.Lock()
	// defer sessionUpdate.sqlStore.mutex.Unlock()
	// var err error
	// if sessionUpdate.sqlStore.dialect == "sqlserver" {
	// 	_, err = sessionUpdate.sqlStore.db.Exec(mssqlPutSessionQuery, sessionUpdate.sqlStore.JID, sessionUpdate.address, sessionUpdate.session)
	// } else {
	// 	_, err = sessionUpdate.sqlStore.db.Exec(sqlitePutSessionQuery, sessionUpdate.sqlStore.JID, sessionUpdate.address, sessionUpdate.session)
	// }
	// return err
	_, err := sqlInstance.Exec(mssqlPutSessionQuery, sessionUpdate.jid, sessionUpdate.address, sessionUpdate.session)
	return err
}

var _ store.IdentityStore = (*SQLStore)(nil)
var _ store.SessionStore = (*SQLStore)(nil)
var _ store.PreKeyStore = (*SQLStore)(nil)
var _ store.SenderKeyStore = (*SQLStore)(nil)
var _ store.AppStateSyncKeyStore = (*SQLStore)(nil)
var _ store.AppStateStore = (*SQLStore)(nil)
var _ store.ContactStore = (*SQLStore)(nil)

const (
	mssqlPutIdentityQuery = `
	MERGE INTO whatsmeow_identity_keys AS target
	USING (VALUES (@p1, @p2, @p3)) AS source (our_jid, their_id, identity_info)
	   ON (target.our_jid = source.our_jid AND target.their_id = source.their_id)
	WHEN MATCHED THEN
	   UPDATE SET target.identity_info = source.identity_info
	WHEN NOT MATCHED THEN
	   INSERT (our_jid, their_id, identity_info)
	   VALUES (source.our_jid, source.their_id, source.identity_info);
	`
	sqlitePutIdentityQuery = `
	INSERT INTO whatsmeow_identity_keys (our_jid, their_id, identity) VALUES (@p1, @p2, @p3)
		ON CONFLICT (our_jid, their_id) DO UPDATE SET identity=excluded.identity
	`
	deleteAllIdentitiesQuery = `DELETE FROM whatsmeow_identity_keys WHERE our_jid=@p1 AND their_id LIKE @p2`
	deleteIdentityQuery      = `DELETE FROM whatsmeow_identity_keys WHERE our_jid=@p1 AND their_id=@p2`
	sqliteGetIdentityQuery   = `SELECT identity FROM whatsmeow_identity_keys WHERE our_jid=@p1 AND their_id=@p2`
	mssqlGetIdentityQuery    = `SELECT identity_info FROM whatsmeow_identity_keys WITH (NOLOCK) WHERE our_jid=@p1 AND their_id=@p2`
)

func (s *SQLStore) PutIdentity(address string, key [32]byte) error {
	identitiesChannel <- identityUpdate{
		address: address,
		key:     key,
		jid:     s.JID,
	}
	return nil
}

func (s *SQLStore) DeleteAllIdentities(phone string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	_, err := s.db.Exec(deleteAllIdentitiesQuery, s.JID, phone+":%")
	return err
}

func (s *SQLStore) DeleteIdentity(address string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	_, err := s.db.Exec(deleteAllIdentitiesQuery, s.JID, address)
	return err
}

func (s *SQLStore) IsTrustedIdentity(address string, key [32]byte) (bool, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	var existingIdentity []byte
	var err error
	if s.dialect == "sqlserver" {
		err = s.db.QueryRow(mssqlGetIdentityQuery, s.JID, address).Scan(&existingIdentity)
	} else {
		err = s.db.QueryRow(sqliteGetIdentityQuery, s.JID, address).Scan(&existingIdentity)
	}
	if errors.Is(err, sql.ErrNoRows) {
		// Trust if not known, it'll be saved automatically later
		return true, nil
	} else if err != nil {
		return false, err
	} else if len(existingIdentity) != 32 {
		return false, ErrInvalidLength
	}
	return *(*[32]byte)(existingIdentity) == key, nil
}

const (
	getSessionQuery       = `SELECT session FROM whatsmeow_sessions WITH (NOLOCK) WHERE our_jid=@p1 AND their_id=@p2`
	sqliteHasSessionQuery = `SELECT true FROM whatsmeow_sessions WHERE our_jid=@p1 AND their_id=@p2`
	mssqlHasSessionQuery  = `SELECT 1 FROM whatsmeow_sessions WITH (NOLOCK) WHERE our_jid=@p1 AND their_id=@p2`
	mssqlHasDeviceManager = `SELECT 1 FROM whatsmeow_device WITH (NOLOCK) WHERE manager_id=@p1`
	sqlitePutSessionQuery = `
		INSERT INTO whatsmeow_sessions (our_jid, their_id, session) VALUES (@p1, @p2, @p3)
		ON CONFLICT (our_jid, their_id) DO UPDATE SET session=excluded.session
	`
	mssqlPutSessionQuery = `
	MERGE INTO whatsmeow_sessions AS target
	USING (VALUES (@p1, @p2, @p3)) AS source (our_jid, their_id, session)
	ON (target.our_jid = source.our_jid AND target.their_id = source.their_id)
	WHEN MATCHED THEN
    	UPDATE SET target.session = source.session
	WHEN NOT MATCHED THEN
    	INSERT (our_jid, their_id, session)
    	VALUES (source.our_jid, source.their_id, source.session);
	`
	deleteAllSessionsQuery = `DELETE FROM whatsmeow_sessions WHERE our_jid=@p1 AND their_id LIKE @p2`
	deleteSessionQuery     = `DELETE FROM whatsmeow_sessions WHERE our_jid=@p1 AND their_id=@p2`
)

func (s *SQLStore) GetSession(address string) (session []byte, err error) {
	err = s.db.QueryRow(getSessionQuery, s.JID, address).Scan(&session)
	if errors.Is(err, sql.ErrNoRows) {
		err = nil
	}
	return
}

func (s *SQLStore) HasSession(address string) (has bool, err error) {
	if s.dialect == "sqlserver" {
		err = s.db.QueryRow(mssqlHasSessionQuery, s.JID, address).Scan(&has)
	} else {
		err = s.db.QueryRow(sqliteHasSessionQuery, s.JID, address).Scan(&has)
	}
	if errors.Is(err, sql.ErrNoRows) {
		err = nil
	}
	return
}

func (s *SQLStore) PutSession(address string, session []byte) error {
	//res := make(chan error, 1)
	sessionsChannel <- sessionUpdate{
		address: address,
		session: session,
		jid:     s.JID,
		//	err:      res,
	}
	// for err := range res {
	// 	return err
	// }
	return nil
}

func (s *SQLStore) DeleteAllSessions(phone string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	_, err := s.db.Exec(deleteAllSessionsQuery, s.JID, phone+":%")
	return err
}

func (s *SQLStore) DeleteSession(address string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	_, err := s.db.Exec(deleteSessionQuery, s.JID, address)
	return err
}

const (
	getLastPreKeyIDQuery              = `SELECT MAX(key_id) FROM whatsmeow_pre_keys WITH (NOLOCK) WHERE jid=@p1`
	mssqlInsertPreKeyQuery            = `INSERT INTO whatsmeow_pre_keys (jid, key_id, key_info, uploaded) VALUES (@p1, @p2, @p3, @p4)`
	sqliteInsertPreKeyQuery           = `INSERT INTO whatsmeow_pre_keys (jid, key_id, key, uploaded) VALUES (@p1, @p2, @p3, @p4)`
	sqliteGetUnuploadedPreKeysQuery   = `SELECT key_id, key FROM whatsmeow_pre_keys WHERE jid=@p1 AND uploaded=false ORDER BY key_id LIMIT @p2`
	mssqlGetUnuploadedPreKeysQuery    = `SELECT TOP 1 key_id, key_info FROM whatsmeow_pre_keys WITH (NOLOCK) WHERE jid=@p2 AND uploaded=0 ORDER BY key_id`
	sqliteGetPreKeyQuery              = `SELECT key_id, key FROM whatsmeow_pre_keys WHERE jid=@p1 AND key_id=@p2`
	mssqlGetPreKeyQuery               = `SELECT key_id, key_info FROM whatsmeow_pre_keys WITH (NOLOCK) WHERE jid=@p1 AND key_id=@p2`
	deletePreKeyQuery                 = `DELETE FROM whatsmeow_pre_keys WHERE jid=@p1 AND key_id=@p2`
	mssqlMarkPreKeysAsUploadedQuery   = `UPDATE whatsmeow_pre_keys SET uploaded=1 WHERE jid=@p1 AND key_id<=@p2`
	sqliteMarkPreKeysAsUploadedQuery  = `UPDATE whatsmeow_pre_keys SET uploaded=true WHERE jid=@p1 AND key_id<=@p2`
	sqliteGetUploadedPreKeyCountQuery = `SELECT COUNT(*) FROM whatsmeow_pre_keys WHERE jid=@p1 AND uploaded=true`
	mssqlGetUploadedPreKeyCountQuery  = `SELECT COUNT(*) FROM whatsmeow_pre_keys WITH (NOLOCK) WHERE jid=@p1 AND uploaded=1`
)

func (s *SQLStore) genOnePreKey(id uint32, markUploaded bool) (*keys.PreKey, error) {
	key := keys.NewPreKey(id)
	s.mutex.Lock()
	defer s.mutex.Unlock()
	var err error
	if s.dialect == "sqlserver" {
		_, err = s.db.Exec(mssqlInsertPreKeyQuery, s.JID, key.KeyID, key.Priv[:], markUploaded)
	} else {
		_, err = s.db.Exec(sqliteInsertPreKeyQuery, s.JID, key.KeyID, key.Priv[:], markUploaded)
	}

	return key, err
}

func (s *SQLStore) getNextPreKeyID() (uint32, error) {
	var lastKeyID sql.NullInt32
	err := s.db.QueryRow(getLastPreKeyIDQuery, s.JID).Scan(&lastKeyID)
	if err != nil {
		return 0, fmt.Errorf("failed to query next prekey ID: %w", err)
	}
	return uint32(lastKeyID.Int32) + 1, nil
}

func (s *SQLStore) GenOnePreKey() (*keys.PreKey, error) {
	s.preKeyLock.Lock()
	defer s.preKeyLock.Unlock()
	nextKeyID, err := s.getNextPreKeyID()
	if err != nil {
		return nil, err
	}
	return s.genOnePreKey(nextKeyID, true)
}

func (s *SQLStore) GetOrGenPreKeys(count uint32) ([]*keys.PreKey, error) {
	s.preKeyLock.Lock()
	defer s.preKeyLock.Unlock()
	var err error
	var res *sql.Rows
	if s.dialect == "sqlserver" {
		res, err = s.db.Query(mssqlGetUnuploadedPreKeysQuery, count, s.JID)
	} else {
		res, err = s.db.Query(sqliteGetUnuploadedPreKeysQuery, s.JID, count)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query existing prekeys: %w", err)
	}
	newKeys := make([]*keys.PreKey, count)
	var existingCount uint32
	for res.Next() {
		var key *keys.PreKey
		key, err = scanPreKey(res)
		if err != nil {
			return nil, err
		} else if key != nil {
			newKeys[existingCount] = key
			existingCount++
		}
	}

	if existingCount < uint32(len(newKeys)) {
		var nextKeyID uint32
		nextKeyID, err = s.getNextPreKeyID()
		if err != nil {
			return nil, err
		}
		for i := existingCount; i < count; i++ {
			newKeys[i], err = s.genOnePreKey(nextKeyID, false)
			if err != nil {
				return nil, fmt.Errorf("failed to generate prekey: %w", err)
			}
			nextKeyID++
		}
	}

	return newKeys, nil
}

func scanPreKey(row scannable) (*keys.PreKey, error) {
	var priv []byte
	var id uint32
	err := row.Scan(&id, &priv)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	} else if err != nil {
		return nil, err
	} else if len(priv) != 32 {
		return nil, ErrInvalidLength
	}
	return &keys.PreKey{
		KeyPair: *keys.NewKeyPairFromPrivateKey(*(*[32]byte)(priv)),
		KeyID:   id,
	}, nil
}

func (s *SQLStore) GetPreKey(id uint32) (*keys.PreKey, error) {
	if s.dialect == "sqlserver" {
		return scanPreKey(s.db.QueryRow(mssqlGetPreKeyQuery, s.JID, id))
	}
	return scanPreKey(s.db.QueryRow(sqliteGetPreKeyQuery, s.JID, id))
}

func (s *SQLStore) RemovePreKey(id uint32) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	_, err := s.db.Exec(deletePreKeyQuery, s.JID, id)
	return err
}

func (s *SQLStore) MarkPreKeysAsUploaded(upToID uint32) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.dialect == "sqlserver" {
		_, err := s.db.Exec(mssqlMarkPreKeysAsUploadedQuery, s.JID, upToID)
		return err
	}
	_, err := s.db.Exec(sqliteMarkPreKeysAsUploadedQuery, s.JID, upToID)
	return err
}

func (s *SQLStore) UploadedPreKeyCount() (count int, err error) {
	if s.dialect == "sqlserver" {
		err = s.db.QueryRow(mssqlGetUploadedPreKeyCountQuery, s.JID).Scan(&count)
		return
	}
	err = s.db.QueryRow(sqliteGetUploadedPreKeyCountQuery, s.JID).Scan(&count)
	return
}

const (
	getSenderKeyQuery       = `SELECT sender_key FROM whatsmeow_sender_keys WITH (NOLOCK) WHERE our_jid=@p1 AND chat_id=@p2 AND sender_id=@p3`
	sqlitePutSenderKeyQuery = `
		INSERT INTO whatsmeow_sender_keys (our_jid, chat_id, sender_id, sender_key) VALUES (@p1, @p2, @p3, @p4)
		ON CONFLICT (our_jid, chat_id, sender_id) DO UPDATE SET sender_key=excluded.sender_key
	`
	mssqlPutSenderKeyQuery = `
		MERGE INTO whatsmeow_sender_keys AS target
		USING (VALUES (@p1, @p2, @p3, @p4)) AS source (our_jid, chat_id, sender_id, sender_key)
		ON (target.our_jid = source.our_jid AND target.chat_id = source.chat_id AND target.sender_id = source.sender_id)
		WHEN MATCHED THEN
			UPDATE SET target.sender_key = source.sender_key
		WHEN NOT MATCHED THEN
			INSERT (our_jid, chat_id, sender_id, sender_key)
			VALUES (source.our_jid, source.chat_id, source.sender_id, source.sender_key);
	`
)

func (s *SQLStore) PutSenderKey(group, user string, session []byte) error {
	res := make(chan error, 1)
	senderkeysChannel <- senderkeyUpdate{
		group:    group,
		user:     user,
		session:  session,
		sqlStore: s,
		err:      res,
	}
	for err := range res {
		return err
	}
	return nil
}

func (s *SQLStore) GetSenderKey(group, user string) (key []byte, err error) {
	err = s.db.QueryRow(getSenderKeyQuery, s.JID, group, user).Scan(&key)
	if errors.Is(err, sql.ErrNoRows) {
		err = nil
	}
	return
}

const (
	sqlitePutAppStateSyncKeyQuery = `
		INSERT INTO whatsmeow_app_state_sync_keys (jid, key_id, key_data, timestamp, fingerprint) VALUES (@p1, @p2, @p3, @p4, @p5)
		ON CONFLICT (jid, key_id) DO UPDATE
			SET key_data=excluded.key_data, timestamp=excluded.timestamp, fingerprint=excluded.fingerprint
			WHERE excluded.timestamp > whatsmeow_app_state_sync_keys.timestamp
	`
	mssqlPutAppStateSyncKeyQuery = `
		MERGE INTO whatsmeow_app_state_sync_keys AS target
		USING (VALUES (@p1, @p2, @p3, @p4, @p5)) AS source (jid, key_id, key_data, timestamp_info, fingerprint)
		ON (target.jid = source.jid AND target.key_id = source.key_id)
		WHEN MATCHED AND source.timestamp_info > target.timestamp_info THEN
			UPDATE SET target.key_data = source.key_data, target.timestamp_info = source.timestamp_info, target.fingerprint = source.fingerprint
		WHEN NOT MATCHED THEN
			INSERT (jid, key_id, key_data, timestamp_info, fingerprint)
			VALUES (source.jid, source.key_id, source.key_data, source.timestamp_info, source.fingerprint);
	`
	sqliteGetAppStateSyncKeyQuery         = `SELECT key_data, timestamp, fingerprint FROM whatsmeow_app_state_sync_keys WHERE jid=@p1 AND key_id=@p2`
	mssqlGetAppStateSyncKeyQuery          = `SELECT key_data, timestamp_info, fingerprint FROM whatsmeow_app_state_sync_keys WITH (NOLOCK) WHERE jid=@p1 AND key_id=@p2`
	sqliteGetLatestAppStateSyncKeyIDQuery = `SELECT key_id FROM whatsmeow_app_state_sync_keys WHERE jid=@p1 ORDER BY timestamp DESC LIMIT 1`
	mssqlGetLatestAppStateSyncKeyIDQuery  = `SELECT TOP 1 key_id FROM whatsmeow_app_state_sync_keys WITH (NOLOCK) WHERE jid=@p1 ORDER BY timestamp_info DESC`
)

func (s *SQLStore) PutAppStateSyncKey(id []byte, key store.AppStateSyncKey) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.dialect == "sqlserver" {
		_, err := s.db.Exec(mssqlPutAppStateSyncKeyQuery, s.JID, id, key.Data, key.Timestamp, key.Fingerprint)
		return err
	}
	_, err := s.db.Exec(sqlitePutAppStateSyncKeyQuery, s.JID, id, key.Data, key.Timestamp, key.Fingerprint)
	return err
}

func (s *SQLStore) GetAppStateSyncKey(id []byte) (*store.AppStateSyncKey, error) {
	var key store.AppStateSyncKey
	var err error
	if s.dialect == "sqlserver" {
		err = s.db.QueryRow(mssqlGetAppStateSyncKeyQuery, s.JID, id).Scan(&key.Data, &key.Timestamp, &key.Fingerprint)
	} else {
		err = s.db.QueryRow(sqliteGetAppStateSyncKeyQuery, s.JID, id).Scan(&key.Data, &key.Timestamp, &key.Fingerprint)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return &key, err
}

func (s *SQLStore) GetLatestAppStateSyncKeyID() ([]byte, error) {
	var keyID []byte
	var err error
	if s.dialect == "sqlserver" {
		err = s.db.QueryRow(mssqlGetLatestAppStateSyncKeyIDQuery, s.JID).Scan(&keyID)
	} else {
		err = s.db.QueryRow(sqliteGetLatestAppStateSyncKeyIDQuery, s.JID).Scan(&keyID)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return keyID, err
}

const (
	sqlitePutAppStateVersionQuery = `
		INSERT INTO whatsmeow_app_state_version (jid, name, version, hash) VALUES (@p1, @p2, @p3, @p4)
		ON CONFLICT (jid, name) DO UPDATE SET version=excluded.version, hash=excluded.hash
	`
	mssqlPutAppStateVersionQuery = `
		MERGE INTO whatsmeow_app_state_version AS target
		USING (VALUES (@p1, @p2, @p3, @p4)) AS source (jid, name, version_info, hash)
		ON (target.jid = source.jid AND target.name = source.name)
		WHEN MATCHED THEN
			UPDATE SET target.version_info = source.version_info, target.hash = source.hash
		WHEN NOT MATCHED THEN
			INSERT (jid, name, version_info, hash)
			VALUES (source.jid, source.name, source.version_info, source.hash);
	`
	sqliteGetAppStateVersionQuery           = `SELECT version, hash FROM whatsmeow_app_state_version WHERE jid=@p1 AND name=@p2`
	mssqlGetAppStateVersionQuery            = `SELECT version_info, hash FROM whatsmeow_app_state_version WITH (NOLOCK) WHERE jid=@p1 AND name=@p2`
	deleteAppStateVersionQuery              = `DELETE FROM whatsmeow_app_state_version WHERE jid=@p1 AND name=@p2`
	sqlitePutAppStateMutationMACsQuery      = `INSERT INTO whatsmeow_app_state_mutation_macs (jid, name, version, index_mac, value_mac) VALUES `
	mssqlPutAppStateMutationMACsQuery       = `INSERT INTO whatsmeow_app_state_mutation_macs (jid, name, version_info, index_mac, value_mac) VALUES `
	deleteAppStateMutationMACsQueryPostgres = `DELETE FROM whatsmeow_app_state_mutation_macs WHERE jid=@p1 AND name=@p2 AND index_mac=ANY(@p3::bytea[])`
	deleteAppStateMutationMACsQueryGeneric  = `DELETE FROM whatsmeow_app_state_mutation_macs WHERE jid=@p1 AND name=@p2 AND index_mac IN `
	sqliteGetAppStateMutationMACQuery       = `SELECT value_mac FROM whatsmeow_app_state_mutation_macs WHERE jid=@p1 AND name=@p2 AND index_mac=@p3 ORDER BY version DESC LIMIT 1`
	mssqlGetAppStateMutationMACQuery        = `SELECT TOP 1 value_mac FROM whatsmeow_app_state_mutation_macs WITH (NOLOCK) WHERE jid=@p1 AND name=@p2 AND index_mac=@p3 ORDER BY version_info DESC`
)

func (s *SQLStore) PutAppStateVersion(name string, version uint64, hash [128]byte) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.dialect == "sqlserver" {
		_, err := s.db.Exec(mssqlPutAppStateVersionQuery, s.JID, name, version, hash[:])
		return err
	}
	_, err := s.db.Exec(sqlitePutAppStateVersionQuery, s.JID, name, version, hash[:])
	return err
}

func (s *SQLStore) GetAppStateVersion(name string) (version uint64, hash [128]byte, err error) {
	var uncheckedHash []byte
	if s.dialect == "sqlserver" {
		err = s.db.QueryRow(mssqlGetAppStateVersionQuery, s.JID, name).Scan(&version, &uncheckedHash)
	} else {
		err = s.db.QueryRow(sqliteGetAppStateVersionQuery, s.JID, name).Scan(&version, &uncheckedHash)
	}
	if errors.Is(err, sql.ErrNoRows) {
		// version will be 0 and hash will be an empty array, which is the correct initial state
		err = nil
	} else if err != nil {
		// There's an error, just return it
	} else if len(uncheckedHash) != 128 {
		// This shouldn't happen
		err = ErrInvalidLength
	} else {
		// No errors, convert hash slice to array
		hash = *(*[128]byte)(uncheckedHash)
	}
	return
}

func (s *SQLStore) DeleteAppStateVersion(name string) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	_, err := s.db.Exec(deleteAppStateVersionQuery, s.JID, name)
	return err
}

type execable interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
}

func (s *SQLStore) putAppStateMutationMACs(tx execable, name string, version uint64, mutations []store.AppStateMutationMAC) error {
	values := make([]interface{}, 3+len(mutations)*2)
	queryParts := make([]string, len(mutations))
	values[0] = s.JID
	values[1] = name
	values[2] = version
	placeholderSyntax := "(@p1, @p2, @p3, @p%d, @p%d)"
	if s.dialect == "sqlite3" {
		placeholderSyntax = "(?1, ?2, ?3, ?%d, ?%d)"
	}
	for i, mutation := range mutations {
		baseIndex := 3 + i*2
		values[baseIndex] = mutation.IndexMAC
		values[baseIndex+1] = mutation.ValueMAC
		queryParts[i] = fmt.Sprintf(placeholderSyntax, baseIndex+1, baseIndex+2)
	}
	if s.dialect == "sqlserver" {
		_, err := tx.Exec(mssqlPutAppStateMutationMACsQuery+strings.Join(queryParts, ","), values...)
		return err
	}
	_, err := tx.Exec(sqlitePutAppStateMutationMACsQuery+strings.Join(queryParts, ","), values...)
	return err
}

const mutationBatchSize = 400

func (s *SQLStore) PutAppStateMutationMACs(name string, version uint64, mutations []store.AppStateMutationMAC) error {
	if len(mutations) > mutationBatchSize {
		tx, err := s.db.Begin()
		if err != nil {
			return fmt.Errorf("failed to start transaction: %w", err)
		}
		for i := 0; i < len(mutations); i += mutationBatchSize {
			var mutationSlice []store.AppStateMutationMAC
			if len(mutations) > i+mutationBatchSize {
				mutationSlice = mutations[i : i+mutationBatchSize]
			} else {
				mutationSlice = mutations[i:]
			}
			err = s.putAppStateMutationMACs(tx, name, version, mutationSlice)
			if err != nil {
				_ = tx.Rollback()
				return err
			}
		}
		err = tx.Commit()
		if err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}
		return nil
	} else if len(mutations) > 0 {
		return s.putAppStateMutationMACs(s.db, name, version, mutations)
	}
	return nil
}

func (s *SQLStore) DeleteAppStateMutationMACs(name string, indexMACs [][]byte) (err error) {
	if len(indexMACs) == 0 {
		return
	}
	if s.dialect == "postgres" && PostgresArrayWrapper != nil {
		_, err = s.db.Exec(deleteAppStateMutationMACsQueryPostgres, s.JID, name, PostgresArrayWrapper(indexMACs))
	} else {
		args := make([]interface{}, 2+len(indexMACs))
		args[0] = s.JID
		args[1] = name
		queryParts := make([]string, len(indexMACs))
		for i, item := range indexMACs {
			args[2+i] = item
			queryParts[i] = fmt.Sprintf("@p%d", i+3)
		}
		_, err = s.db.Exec(deleteAppStateMutationMACsQueryGeneric+"("+strings.Join(queryParts, ",")+")", args...)
	}
	return
}

func (s *SQLStore) GetAppStateMutationMAC(name string, indexMAC []byte) (valueMAC []byte, err error) {
	if s.dialect == "sqlserver" {
		err = s.db.QueryRow(mssqlGetAppStateMutationMACQuery, s.JID, name, indexMAC).Scan(&valueMAC)
	} else {
		err = s.db.QueryRow(sqliteGetAppStateMutationMACQuery, s.JID, name, indexMAC).Scan(&valueMAC)
	}
	if errors.Is(err, sql.ErrNoRows) {
		err = nil
	}
	return
}

const (
	sqlitePutContactNameQuery = `
		INSERT INTO whatsmeow_contacts (our_jid, their_jid, first_name, full_name) VALUES (@p1, @p2, @p3, @p4)
		ON CONFLICT (our_jid, their_jid) DO UPDATE SET first_name=excluded.first_name, full_name=excluded.full_name
	`
	mssqlPutContactNameQuery = `
		MERGE INTO whatsmeow_contacts AS target
		USING (VALUES (@p1, @p2, @p3, @p4)) AS source (our_jid, their_jid, first_name, full_name)
		ON (target.our_jid = source.our_jid AND target.their_jid = source.their_jid)
		WHEN MATCHED THEN
			UPDATE SET target.first_name = source.first_name, target.full_name = source.full_name
		WHEN NOT MATCHED THEN
			INSERT (our_jid, their_jid, first_name, full_name)
			VALUES (source.our_jid, source.their_jid, source.first_name, source.full_name);
	`
	sqlitePutManyContactNamesQuery = `
		INSERT INTO whatsmeow_contacts (our_jid, their_jid, first_name, full_name)
		VALUES %s
		ON CONFLICT (our_jid, their_jid) DO UPDATE SET first_name=excluded.first_name, full_name=excluded.full_name
	`
	mssqlPutManyContactNamesQuery = `
		MERGE INTO whatsmeow_contacts AS target
		USING (VALUES %s) AS source (our_jid, their_jid, first_name, full_name)
		ON (target.our_jid = source.our_jid AND target.their_jid = source.their_jid)
		WHEN MATCHED THEN
			UPDATE SET target.first_name = source.first_name, target.full_name = source.full_name
		WHEN NOT MATCHED THEN
			INSERT (our_jid, their_jid, first_name, full_name)
			VALUES (source.our_jid, source.their_jid, source.first_name, source.full_name);
	`
	sqlitePutPushNameQuery = `
		INSERT INTO whatsmeow_contacts (our_jid, their_jid, push_name) VALUES (@p1, @p2, @p3)
		ON CONFLICT (our_jid, their_jid) DO UPDATE SET push_name=excluded.push_name
	`
	mssqlPutPushNameQuery = `
		MERGE INTO whatsmeow_contacts AS target
		USING (VALUES (@p1, @p2, @p3)) AS source (our_jid, their_jid, push_name)
		ON (target.our_jid = source.our_jid AND target.their_jid = source.their_jid)
		WHEN MATCHED THEN
			UPDATE SET target.push_name = source.push_name
		WHEN NOT MATCHED THEN
			INSERT (our_jid, their_jid, push_name)
			VALUES (source.our_jid, source.their_jid, source.push_name);
	`
	sqlitePutBusinessNameQuery = `
		INSERT INTO whatsmeow_contacts (our_jid, their_jid, business_name) VALUES (@p1, @p2, @p3)
		ON CONFLICT (our_jid, their_jid) DO UPDATE SET business_name=excluded.business_name
	`
	mssqlPutBusinessNameQuery = `
		MERGE INTO whatsmeow_contacts AS target
		USING (VALUES (@p1, @p2, @p3)) AS source (our_jid, their_jid, business_name)
		ON (target.our_jid = source.our_jid AND target.their_jid = source.their_jid)
		WHEN MATCHED THEN
			UPDATE SET target.business_name = source.business_name
		WHEN NOT MATCHED THEN
			INSERT (our_jid, their_jid, business_name)
			VALUES (source.our_jid, source.their_jid, source.business_name);
	`
	getContactQuery = `
		SELECT first_name, full_name, push_name, business_name FROM whatsmeow_contacts WITH (NOLOCK) WHERE our_jid=@p1 AND their_jid=@p2
	`
	getAllContactsQuery = `
		SELECT their_jid, first_name, full_name, push_name, business_name FROM whatsmeow_contacts WITH (NOLOCK) WHERE our_jid=@p1
	`
)

func (s *SQLStore) PutPushName(user types.JID, pushName string) (bool, string, error) {
	s.contactCacheLock.Lock()
	defer s.contactCacheLock.Unlock()

	cached, err := s.getContact(user)
	if err != nil {
		return false, "", err
	}
	if cached.PushName != pushName {
		if s.dialect == "sqlserver" {
			_, err = s.db.Exec(mssqlPutPushNameQuery, s.JID, user, pushName)
		} else {
			_, err = s.db.Exec(sqlitePutPushNameQuery, s.JID, user, pushName)
		}
		if err != nil {
			return false, "", err
		}
		previousName := cached.PushName
		cached.PushName = pushName
		cached.Found = true
		return true, previousName, nil
	}
	return false, "", nil
}

func (s *SQLStore) PutBusinessName(user types.JID, businessName string) (bool, string, error) {
	s.contactCacheLock.Lock()
	defer s.contactCacheLock.Unlock()

	cached, err := s.getContact(user)
	if err != nil {
		return false, "", err
	}
	if cached.BusinessName != businessName {
		if s.dialect == "sqlserver" {
			_, err = s.db.Exec(mssqlPutBusinessNameQuery, s.JID, user, businessName)
		} else {
			_, err = s.db.Exec(sqlitePutBusinessNameQuery, s.JID, user, businessName)
		}
		if err != nil {
			return false, "", err
		}
		previousName := cached.BusinessName
		cached.BusinessName = businessName
		cached.Found = true
		return true, previousName, nil
	}
	return false, "", nil
}

func (s *SQLStore) PutContactName(user types.JID, firstName, fullName string) error {
	s.contactCacheLock.Lock()
	defer s.contactCacheLock.Unlock()

	cached, err := s.getContact(user)
	if err != nil {
		return err
	}
	if cached.FirstName != firstName || cached.FullName != fullName {
		if s.dialect == "sqlserver" {
			_, err = s.db.Exec(mssqlPutContactNameQuery, s.JID, user, firstName, fullName)
		} else {
			_, err = s.db.Exec(sqlitePutContactNameQuery, s.JID, user, firstName, fullName)
		}
		if err != nil {
			return err
		}
		cached.FirstName = firstName
		cached.FullName = fullName
		cached.Found = true
	}
	return nil
}

const contactBatchSize = 300

func (s *SQLStore) putContactNamesBatch(tx execable, contacts []store.ContactEntry) error {
	values := make([]interface{}, 1, 1+len(contacts)*3)
	queryParts := make([]string, 0, len(contacts))
	values[0] = s.JID
	placeholderSyntax := "(@p1, @p%d, @p%d, @p%d)"
	if s.dialect == "sqlite3" {
		placeholderSyntax = "(?1, ?%d, ?%d, ?%d)"
	}
	i := 0
	handledContacts := make(map[types.JID]struct{}, len(contacts))
	for _, contact := range contacts {
		if contact.JID.IsEmpty() {
			s.log.Warnf("Empty contact info in mass insert: %+v", contact)
			continue
		}
		// The whole query will break if there are duplicates, so make sure there aren't any duplicates
		_, alreadyHandled := handledContacts[contact.JID]
		if alreadyHandled {
			s.log.Warnf("Duplicate contact info for %s in mass insert", contact.JID)
			continue
		}
		handledContacts[contact.JID] = struct{}{}
		baseIndex := i*3 + 1
		values = append(values, contact.JID.String(), contact.FirstName, contact.FullName)
		queryParts = append(queryParts, fmt.Sprintf(placeholderSyntax, baseIndex+1, baseIndex+2, baseIndex+3))
		i++
	}
	if s.dialect == "sqlserver" {
		_, err := tx.Exec(fmt.Sprintf(mssqlPutManyContactNamesQuery, strings.Join(queryParts, ",")), values...)
		return err
	}
	_, err := tx.Exec(fmt.Sprintf(sqlitePutManyContactNamesQuery, strings.Join(queryParts, ",")), values...)
	return err
}

func (s *SQLStore) PutAllContactNames(contacts []store.ContactEntry) error {
	contactsChannel <- contactUpdate{
		sqlStore: s,
		contacts: contacts,
	}
	return nil
}

func (s *SQLStore) getContact(user types.JID) (*types.ContactInfo, error) {
	cached, ok := s.contactCache[user]
	if ok {
		return cached, nil
	}

	var first, full, push, business sql.NullString
	err := s.db.QueryRow(getContactQuery, s.JID, user).Scan(&first, &full, &push, &business)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}
	info := &types.ContactInfo{
		Found:        err == nil,
		FirstName:    first.String,
		FullName:     full.String,
		PushName:     push.String,
		BusinessName: business.String,
	}
	s.contactCache[user] = info
	return info, nil
}

func (s *SQLStore) GetContact(user types.JID) (types.ContactInfo, error) {
	s.contactCacheLock.Lock()
	info, err := s.getContact(user)
	s.contactCacheLock.Unlock()
	if err != nil {
		return types.ContactInfo{}, err
	}
	return *info, nil
}

func (s *SQLStore) GetAllContacts() (map[types.JID]types.ContactInfo, error) {
	s.contactCacheLock.Lock()
	defer s.contactCacheLock.Unlock()
	rows, err := s.db.Query(getAllContactsQuery, s.JID)
	if err != nil {
		return nil, err
	}
	output := make(map[types.JID]types.ContactInfo, len(s.contactCache))
	for rows.Next() {
		var jid types.JID
		var first, full, push, business sql.NullString
		err = rows.Scan(&jid, &first, &full, &push, &business)
		if err != nil {
			return nil, fmt.Errorf("error scanning row: %w", err)
		}
		info := types.ContactInfo{
			Found:        true,
			FirstName:    first.String,
			FullName:     full.String,
			PushName:     push.String,
			BusinessName: business.String,
		}
		output[jid] = info
		s.contactCache[jid] = &info
	}
	return output, nil
}

const (
	sqlitePutChatSettingQuery = `
		INSERT INTO whatsmeow_chat_settings (our_jid, chat_jid, %[1]s) VALUES (@p1, @p2, @p3)
		ON CONFLICT (our_jid, chat_jid) DO UPDATE SET %[1]s=excluded.%[1]s
	`
	mssqlPutChatSettingQuery = `
		MERGE INTO whatsmeow_chat_settings AS target
		USING (VALUES (@p1, @p2, @p3)) AS source (our_jid, chat_jid, %[1]s)
		ON (target.our_jid = source.our_jid AND target.chat_jid = source.chat_jid)
		WHEN MATCHED THEN
			UPDATE SET target.%[1]s = source.%[1]s
		WHEN NOT MATCHED THEN
			INSERT (our_jid, chat_jid, %[1]s)
			VALUES (source.our_jid, source.chat_jid, source.%[1]s);
	`
	getChatSettingsQuery = `
		SELECT muted_until, pinned, archived FROM whatsmeow_chat_settings WITH (NOLOCK) WHERE our_jid=@p1 AND chat_jid=@p2
	`
)

func (s *SQLStore) PutMutedUntil(chat types.JID, mutedUntil time.Time) error {
	var val int64
	if !mutedUntil.IsZero() {
		val = mutedUntil.Unix()
	}
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.dialect == "sqlserver" {
		_, err := s.db.Exec(fmt.Sprintf(mssqlPutChatSettingQuery, "muted_until"), s.JID, chat, val)
		return err
	}
	_, err := s.db.Exec(fmt.Sprintf(sqlitePutChatSettingQuery, "muted_until"), s.JID, chat, val)
	return err
}

func (s *SQLStore) PutPinned(chat types.JID, pinned bool) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.dialect == "sqlserver" {
		_, err := s.db.Exec(fmt.Sprintf(mssqlPutChatSettingQuery, "pinned"), s.JID, chat, pinned)
		return err
	}
	_, err := s.db.Exec(fmt.Sprintf(sqlitePutChatSettingQuery, "pinned"), s.JID, chat, pinned)
	return err
}

func (s *SQLStore) PutArchived(chat types.JID, archived bool) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.dialect == "sqlserver" {
		_, err := s.db.Exec(fmt.Sprintf(mssqlPutChatSettingQuery, "archived"), s.JID, chat, archived)
		return err
	}
	_, err := s.db.Exec(fmt.Sprintf(sqlitePutChatSettingQuery, "archived"), s.JID, chat, archived)
	return err
}

func (s *SQLStore) GetChatSettings(chat types.JID) (settings types.LocalChatSettings, err error) {
	var mutedUntil int64
	err = s.db.QueryRow(getChatSettingsQuery, s.JID, chat).Scan(&mutedUntil, &settings.Pinned, &settings.Archived)
	if errors.Is(err, sql.ErrNoRows) {
		err = nil
	} else if err != nil {
		return
	} else {
		settings.Found = true
	}
	if mutedUntil != 0 {
		settings.MutedUntil = time.Unix(mutedUntil, 0)
	}
	return
}

const (
	sqlitePutMsgSecret = `
		INSERT INTO whatsmeow_message_secrets (our_jid, chat_jid, sender_jid, message_id, key)
		VALUES (@p1, @p2, @p3, @p4, @p5)
		ON CONFLICT (our_jid, chat_jid, sender_jid, message_id) DO NOTHING
	`
	mssqlPutMsgSecret = `
		INSERT INTO whatsmeow_message_secrets (our_jid, chat_jid, sender_jid, message_id, key_info)
		SELECT @p1, @p2, @p3, @p4, @p5
		WHERE NOT EXISTS (
			SELECT 1
			FROM whatsmeow_message_secrets
			WHERE our_jid = @p1
				AND chat_jid = @p2
				AND sender_jid = @p3
				AND message_id = @p4
		);
	`
	sqliteGetMsgSecret = `
		SELECT key FROM whatsmeow_message_secrets WHERE our_jid=@p1 AND chat_jid=@p2 AND sender_jid=@p3 AND message_id=@p4
	`
	mssqlGetMsgSecret = `
		SELECT key_info FROM whatsmeow_message_secrets WITH (NOLOCK) WHERE our_jid=@p1 AND chat_jid=@p2 AND sender_jid=@p3 AND message_id=@p4
	`
)

func (s *SQLStore) PutMessageSecrets(inserts []store.MessageSecretInsert) (err error) {
	tx, err := s.db.Begin()
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	for _, insert := range inserts {
		if s.dialect == "sqlserver" {
			_, err = tx.Exec(mssqlPutMsgSecret, s.JID, insert.Chat.ToNonAD(), insert.Sender.ToNonAD(), insert.ID, insert.Secret)
		} else {
			_, err = tx.Exec(sqlitePutMsgSecret, s.JID, insert.Chat.ToNonAD(), insert.Sender.ToNonAD(), insert.ID, insert.Secret)
		}
		if err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("failed to insert secret "+s.JID+": %w", err)
		}
	}
	err = tx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return
}

func (s *SQLStore) PutMessageSecret(chat, sender types.JID, id types.MessageID, secret []byte) (err error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	if s.dialect == "sqlserver" {
		_, err = s.db.Exec(mssqlPutMsgSecret, s.JID, chat.ToNonAD(), sender.ToNonAD(), id, secret)
		return
	}
	_, err = s.db.Exec(sqlitePutMsgSecret, s.JID, chat.ToNonAD(), sender.ToNonAD(), id, secret)
	return
}

func (s *SQLStore) GetMessageSecret(chat, sender types.JID, id types.MessageID) (secret []byte, err error) {
	if s.dialect == "sqlserver" {
		err = s.db.QueryRow(mssqlGetMsgSecret, s.JID, chat.ToNonAD(), sender.ToNonAD(), id).Scan(&secret)
	} else {
		err = s.db.QueryRow(sqliteGetMsgSecret, s.JID, chat.ToNonAD(), sender.ToNonAD(), id).Scan(&secret)
	}
	if errors.Is(err, sql.ErrNoRows) {
		err = nil
	}
	return
}

const (
	sqlitePutPrivacyTokens = `
		INSERT INTO whatsmeow_privacy_tokens (our_jid, their_jid, token, timestamp)
		VALUES (@p1, @p2, @p3, @p4)
		ON CONFLICT (our_jid, their_jid) DO UPDATE SET token=EXCLUDED.token, timestamp=EXCLUDED.timestamp
	`
	mssqlPutPrivacyTokens = `
		MERGE INTO whatsmeow_privacy_tokens AS target
		USING (VALUES (@p1, @p2, @p3, @p4)) AS source (our_jid, their_jid, token, timestamp_info)
		ON (target.our_jid = source.our_jid AND target.their_jid = source.their_jid)
		WHEN MATCHED THEN
			UPDATE SET target.token = source.token, target.timestamp_info = source.timestamp_info
		WHEN NOT MATCHED THEN
			INSERT (our_jid, their_jid, token, timestamp_info)
			VALUES (source.our_jid, source.their_jid, source.token, source.timestamp_info);
	`
	sqliteGetPrivacyToken = `SELECT token, timestamp FROM whatsmeow_privacy_tokens WHERE our_jid=@p1 AND their_jid=@p2`
	mssqlGetPrivacyToken  = `SELECT token, timestamp_info FROM whatsmeow_privacy_tokens WITH (NOLOCK) WHERE our_jid=@p1 AND their_jid=@p2`
)

func (s *SQLStore) PutPrivacyTokens(tokens ...store.PrivacyToken) error {
	args := make([]any, 1+len(tokens)*3)
	placeholders := make([]string, len(tokens))
	args[0] = s.JID
	for i, token := range tokens {
		args[i*3+1] = token.User.ToNonAD().String()
		args[i*3+2] = token.Token
		args[i*3+3] = token.Timestamp.Unix()
		placeholders[i] = fmt.Sprintf("(@p1, @p%d, @p%d, @p%d)", i*3+2, i*3+3, i*3+4)
	}
	var query string
	if s.dialect == "sqlserver" {
		query = strings.ReplaceAll(mssqlPutPrivacyTokens, "(@p1, @p2, @p3, @p4)", strings.Join(placeholders, ","))
	} else {
		query = strings.ReplaceAll(sqlitePutPrivacyTokens, "(@p1, @p2, @p3, @p4)", strings.Join(placeholders, ","))
	}
	s.mutex.Lock()
	defer s.mutex.Unlock()
	_, err := s.db.Exec(query, args...)
	return err
}

func (s *SQLStore) GetPrivacyToken(user types.JID) (*store.PrivacyToken, error) {
	var token store.PrivacyToken
	token.User = user.ToNonAD()
	var ts int64
	var err error
	if s.dialect == "sqlserver" {
		err = s.db.QueryRow(mssqlGetPrivacyToken, s.JID, token.User).Scan(&token.Token, &ts)
	} else {
		err = s.db.QueryRow(sqliteGetPrivacyToken, s.JID, token.User).Scan(&token.Token, &ts)
	}
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	} else if err != nil {
		return nil, err
	} else {
		token.Timestamp = time.Unix(ts, 0)
		return &token, nil
	}
}

const (
	getCacheSessionQuery  = `SELECT their_id, session FROM whatsmeow_sessions WITH (NOLOCK) WHERE our_jid=@p1 AND their_id IN `
	getCacheIdentityQuery = `SELECT their_id, identity_info FROM whatsmeow_identity_keys WITH (NOLOCK) WHERE our_jid=@p1 AND their_id IN `
)

func (s *SQLStore) CacheSessions(addresses []string) (final map[string][]byte) {
	query := getCacheSessionQuery + "("
	queryParams := make([]interface{}, len(addresses)+1)
	queryParams[0] = s.JID
	final = make(map[string][]byte)
	for index, address := range addresses {
		if index > 0 {
			query += ","
		}
		query += fmt.Sprintf("@p%d", index+2)
		queryParams[index+1] = address
	}
	query += ")"
	rows, err := s.db.Query(query, queryParams...)
	if err != nil {
		s.log.Errorf(err.Error())
		return
	}
	for rows.Next() {
		var session []byte
		var id string
		rows.Scan(&id, &session)
		final[id] = session
	}
	return
}

func (s *SQLStore) CacheIdentities(addresses []string) (final map[string][32]byte) {
	query := getCacheIdentityQuery + "("
	queryParams := make([]interface{}, len(addresses)+1)
	queryParams[0] = s.JID
	final = make(map[string][32]byte)
	for index, address := range addresses {
		if index > 0 {
			query += ","
		}
		query += fmt.Sprintf("@p%d", index+2)
		queryParams[index+1] = address
	}
	query += ")"
	rows, err := s.db.Query(query, queryParams...)
	if err != nil {
		s.log.Errorf(err.Error())
		return
	}
	for rows.Next() {
		var session []byte
		var id string
		rows.Scan(&id, &session)
		final[id] = *(*[32]byte)(session)
	}
	return
}
