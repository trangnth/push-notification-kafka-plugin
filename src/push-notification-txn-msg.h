/* Copyright (c) 2015-2016 Dovecot authors, see the included COPYING file */

#ifndef PUSH_NOTIFICATION_TXN_MSG_H
#define PUSH_NOTIFICATION_TXN_MSG_H


struct mail_transaction_commit_changes;
struct push_notification_event_config;
struct push_notification_txn;
struct push_notification_txn_event;

ARRAY_DEFINE_TYPE(msg_uid, const uint32_t *);

struct push_notification_txn_msg {
    const char *mailbox;
    uint32_t uid;
    uint32_t uid_validity;

    ARRAY(struct push_notification_txn_event *) eventdata;
    ARRAY_TYPE(msg_uid) uids;

    /* Private */
    unsigned int seq;
};


struct push_notification_txn_msg *
push_notification_txn_msg_create(struct push_notification_txn *txn,
                                 struct mail *mail);
void
push_notification_txn_msg_end(struct push_notification_txn *ptxn,
                              struct mail_transaction_commit_changes *changes);

void *
push_notification_txn_msg_get_eventdata(struct push_notification_txn_msg *msg,
                                        const char *event_name);
void
push_notification_txn_msg_set_eventdata(struct push_notification_txn *txn,
                                        struct push_notification_txn_msg *msg,
                                        struct push_notification_event_config *event,
                                        void *data);
void
push_notification_txn_msg_deinit_eventdata(struct push_notification_txn_msg *msg);


#endif	/* PUSH_NOTIFICATION_TXN_MSG_H */