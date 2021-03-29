// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab
/*
 * Copyright (c) 2018 Tallence AG and the authors
 *
 * This is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software
 * Foundation.  See file COPYING.
 */

#include <time.h>

#include "config.h"
#include "lib.h"
#include "str.h"
#include "array.h"
#include "hash.h"
#include "json-parser.h"
#include "iso8601-date.h"
#include "macros.h"
#include "mail-user.h"

#include "push-notification-drivers.h"
#include "push-notification-events.h"
#include "push-notification-event-message-common.h"
#include "push-notification-txn-mbox.h"
#include "push-notification-txn-msg.h"
#include "push-notification-event-mailboxcreate.h"
#include "push-notification-event-mailboxrename.h"
#include "push-notification-event-flagsset.h"
#include "push-notification-event-flagsclear.h"
#include "push-notification-event-messagenew.h"
#include "push-notification-event-messageappend.h"

#include "push-notification-kafka-plugin.h"
#include "push-notification-kafka-driver.h"

bool str_starts_with(const char *str, const char *prefix) {
  if (str == NULL || prefix == NULL)
    return FALSE;

  for (;; str++, prefix++) {
    if (!*prefix)
      return TRUE;
    else if (*str != *prefix)
      return FALSE;
  }

  return FALSE;
}

string_t *push_notification_driver_kafka_render_mbox(
    struct push_notification_driver_txn *dtxn,
    struct push_notification_driver_kafka_render_context *render_ctx ATTR_UNUSED,
    struct push_notification_txn_mbox *mbox, struct push_notification_txn_event *const *event) {
  struct mail_user *user = dtxn->ptxn->muser;
  const char *event_name = (*event)->event->event->name;

  string_t *str = str_new(dtxn->ptxn->pool, 512);

  str_append(str, "{\"user\":\"");
  json_append_escaped(str, user->username);
  str_append(str, "\",\"mailbox\":\"");
  json_append_escaped(str, mbox->mailbox);
  str_printfa(str, "\",\"event\":\"%s", event_name);

  if (strcmp(push_notification_event_mailboxcreate.name, event_name) == 0) {
    struct push_notification_event_mailboxcreate_data *data = (*event)->data;
    str_printfa(str, "\",\"uidvalidity\":%u", data->uid_validity);
  } else if (strcmp(push_notification_event_mailboxrename.name, event_name) == 0) {
    struct push_notification_event_mailboxrename_data *data = (*event)->data;
    str_append(str, "\",\"oldMailbox\":\"");
    json_append_escaped(str, data->old_mbox);
    str_printfa(str, "\"");
  }
  str_append(str, "}");

  return str;
}

bool write_flags(enum mail_flags flags, string_t *str) {
  bool flag_written = FALSE;
  if (str == NULL) {
    return FALSE;
  }
  if ((flags & MAIL_ANSWERED) != 0) {
    str_append(str, "\"\\\\Answered\"");
    flag_written = TRUE;
  }
  if ((flags & MAIL_FLAGGED) != 0) {
    if (flag_written) {
      str_append(str, ",");
    }
    str_append(str, "\"\\\\Flagged\"");
    flag_written = TRUE;
  }
  if ((flags & MAIL_DELETED) != 0) {
    if (flag_written) {
      str_append(str, ",");
    }
    str_append(str, "\"\\\\Deleted\"");
    flag_written = TRUE;
  }
  if ((flags & MAIL_SEEN) != 0) {
    if (flag_written) {
      str_append(str, ",");
    }
    str_append(str, "\"\\\\Seen\"");
    flag_written = TRUE;
  }
  if ((flags & MAIL_DRAFT) != 0) {
    if (flag_written) {
      str_append(str, ",");
    }
    str_append(str, "\"\\\\Draft\"");
    flag_written = TRUE;
  }
  return flag_written;
}

string_t *write_msg_prefix(struct push_notification_driver_txn *dtxn, const char *event_name,
                           struct push_notification_txn_msg *msg) {
  string_t *str = str_new(dtxn->ptxn->pool, 512);
  struct push_notification_driver_kafka_context *ctx =
      (struct push_notification_driver_kafka_context *)dtxn->duser->context;

  str_append(str, "{\"user\":\"");
  json_append_escaped(str, dtxn->ptxn->muser->username);
  str_append(str, "\",");

  if (ctx->userdb_json != NULL) {
    str_append(str, ctx->userdb_json);
  }

  str_append(str, "\"mailbox\":\"");
  json_append_escaped(str, msg->mailbox);
  str_printfa(str, "\",\"event\":\"%s\",\"uidvalidity\":%u,\"uid\":%u", event_name, msg->uid_validity, msg->uid);
  return str;
}

string_t *write_flags_event(struct push_notification_driver_txn *dtxn,
                            struct push_notification_driver_kafka_render_context *render_ctx, const char *event_name,
                            struct push_notification_txn_msg *msg, enum mail_flags flags,
                            ARRAY_TYPE(keywords) * keywords, enum mail_flags flags_old,
                            ARRAY_TYPE(keywords) * keywords_old) {
  string_t *str = write_msg_prefix(dtxn, event_name, msg);

  bool flag_written = FALSE;
  if (render_ctx->send_flags && flags > 0) {
    str_append(str, ",\"flags\":[");
    flag_written |= write_flags(flags, str);
    str_append(str, "]");
  }

  if (render_ctx->send_flags && flags_old > 0) {
    str_append(str, ",\"oldFlags\":[");
    flag_written |= write_flags(flags_old, str);
    str_append(str, "]");
  }

  bool keyword_written = FALSE;
  const char *const *keyword;
  int i = 0;
  if (keywords != NULL && array_is_created(keywords) && array_not_empty(keywords)) {
    str_append(str, ",\"keywords\":[");
    array_foreach(keywords, keyword) {
      if (str_starts_with(*keyword, render_ctx->keyword_prefix)) {
        if (i > 0) {
          str_append(str, ",\"");
        } else {
          str_append(str, "\"");
        }
        json_append_escaped(str, *keyword);
        str_append(str, "\"");
        i++;
        keyword_written |= TRUE;
      }
    }
    str_append(str, "]");
  }
  i = 0;
  if (keywords_old != NULL && array_is_created(keywords_old) && array_not_empty(keywords_old)) {
    str_append(str, ",\"oldKeywords\":[");
    array_foreach(keywords_old, keyword) {
#ifdef DEBUG
      i_debug("%swrite_flags_event keyword=%s", LOG_LABEL, *keyword);
#endif
      if (str_starts_with(*keyword, render_ctx->keyword_prefix)) {
        if (i > 0) {
          str_append(str, ",\"");
        } else {
          str_append(str, "\"");
        }
        json_append_escaped(str, *keyword);
        str_append(str, "\"");
        i++;
        keyword_written |= TRUE;
      }
    }
    str_append(str, "]");
  }

  str_append(str, "}");

  if (flag_written || keyword_written) {
    return str;
  }

  // nothing written, send no event

  return NULL;
}

string_t *write_event_messagenew(struct push_notification_driver_txn *dtxn, struct push_notification_txn_msg *msg,
                                 struct push_notification_txn_event *const *event) {
  struct push_notification_event_messagenew_data *data = (*event)->data;
  string_t *str = write_msg_prefix(dtxn, (*event)->event->event->name, msg);

  if (data->date != -1) {
    struct tm *tm = gmtime(&data->date);
    str_printfa(str, ",\"date\":\"%s\",\"date2\":\"%c\"", iso8601_date_create_tm(tm, data->date_tz), data->date_tz);
  }

  if (data->from != NULL) {
    str_append(str, ",\"from\":\"");
    json_append_escaped(str, data->from);
    str_append(str, "\"");
  }

  if (data->snippet != NULL) {
    str_append(str, ",\"snippet\":\"");
    json_append_escaped(str, data->snippet);
    str_append(str, "\"");
  }

  if (data->subject != NULL) {
    str_append(str, ",\"subject\":\"");
    json_append_escaped(str, data->subject);
    str_append(str, "\"");
  }

  if (data->to != NULL) {
    str_append(str, ",\"to\":\"");
    json_append_escaped(str, data->to);
    str_append(str, "\"");
  }

  // if (data->message_id != NULL) {
  //   str_append(str, ",\"message_id\":\"");
  //   json_append_escaped(str, data->message_id);
  //   str_append(str, "\"");
  // }

  str_append(str, "}");
  return str;
}


struct push_notification_event_messageappend_data {
    const char *from;
    const char *to;
    const char *subject;
    time_t date;
    int date_tz;
    const char *snippet;
};

static void
push_notification_event_messageappend_event(struct push_notification_txn *ptxn,
                                            struct push_notification_event_config *ec,
                                            struct push_notification_txn_msg *msg,
                                            struct mail *mail)
{
    struct push_notification_event_messageappend_config *config =
        (struct push_notification_event_messageappend_config *)ec->config;
    struct push_notification_event_messageappend_data *data;
    time_t date;
    int tz;
    const char *value;

    if (!config->flags) {
        return;
    }

    data = push_notification_txn_msg_get_eventdata(msg, EVENT_NAME);
    if (data == NULL) {
        data = p_new(ptxn->pool,
                     struct push_notification_event_messageappend_data, 1);
        push_notification_txn_msg_set_eventdata(ptxn, msg, ec, data);
    }

    if ((data->to == NULL) &&
        (config->flags & PUSH_NOTIFICATION_MESSAGE_HDR_TO) &&
        (mail_get_first_header(mail, "To", &value) >= 0)) {
        data->to = p_strdup(ptxn->pool, value);
    }

    if ((data->from == NULL) &&
        (config->flags & PUSH_NOTIFICATION_MESSAGE_HDR_FROM) &&
        (mail_get_first_header(mail, "From", &value) >= 0)) {
        data->from = p_strdup(ptxn->pool, value);
    }

    if ((data->subject == NULL) &&
        (config->flags & PUSH_NOTIFICATION_MESSAGE_HDR_SUBJECT) &&
        (mail_get_first_header(mail, "Subject", &value) >= 0)) {
        data->subject = p_strdup(ptxn->pool, value);
    }

    if ((data->snippet == NULL) &&
        (config->flags & PUSH_NOTIFICATION_MESSAGE_BODY_SNIPPET) &&
        (mail_get_special(mail, MAIL_FETCH_BODY_SNIPPET, &value) >= 0)) {
        /* [0] contains the snippet algorithm, skip over it */
        i_assert(value[0] != '\0');
        data->snippet = p_strdup(ptxn->pool, value + 1);
    }

    if ((data->date == -1) &&
        (config->flags & PUSH_NOTIFICATION_MESSAGE_HDR_DATE) &&
        (mail_get_date(mail, &date, &tz) >= 0)) {
        data->date = date;
        data->date_tz = tz;
    }
}


string_t *write_event_messageappend(struct push_notification_driver_txn *dtxn, struct push_notification_txn_msg *msg,
                                    struct push_notification_txn_event *const *event) {
  struct push_notification_event_messageappend_data *data = (*event)->data;
  string_t *str = write_msg_prefix(dtxn, (*event)->event->event->name, msg);

  if (data->date != -1) {
    struct tm *tm = gmtime(&data->date);
    str_printfa(str, ",\"date\":\"%s\"", iso8601_date_create_tm(tm, data->date_tz));
  }

  if (data->from != NULL) {
    str_append(str, ",\"from\":\"");
    json_append_escaped(str, data->from);
    str_append(str, "\"");
  }

  if (data->snippet != NULL) {
    str_append(str, ",\"snippet\":\"");
    json_append_escaped(str, data->snippet);
    str_append(str, "\"");
  }

  if (data->subject != NULL) {
    str_append(str, ",\"subject\":\"");
    json_append_escaped(str, data->subject);
    str_append(str, "\"");
  }

  if (data->to != NULL) {
    str_append(str, ",\"to\":\"");
    json_append_escaped(str, data->to);
    str_append(str, "\"");
  }

  // if (data->message_id != NULL) {
  //   str_append(str, ",\"message_id\":\"");
  //   json_append_escaped(str, data->message_id);
  //   str_append(str, "\"");
  // }

  str_append(str, "}");
  return str;
}

string_t *push_notification_driver_kafka_render_msg(struct push_notification_driver_txn *dtxn,
                                                    struct push_notification_driver_kafka_render_context *render_ctx,
                                                    struct push_notification_txn_msg *msg,
                                                    struct push_notification_txn_event *const *event) {
  const char *event_name = (*event)->event->event->name;

  string_t *str = NULL;

  if (strcmp(push_notification_event_flagsset.name, (*event)->event->event->name) == 0) {
    struct push_notification_event_flagsset_data *data = (*event)->data;
    str = write_flags_event(dtxn, render_ctx, event_name, msg, data->flags_set, &data->keywords_set, 0, NULL);
  } else if (strcmp(push_notification_event_flagsclear.name, (*event)->event->event->name) == 0) {
    struct push_notification_event_flagsclear_data *data = (*event)->data;
    str = write_flags_event(dtxn, render_ctx, event_name, msg, data->flags_clear, &data->keywords_clear,
                            data->flags_old, &data->keywords_old);
  } else if (strcmp(push_notification_event_messagenew.name, (*event)->event->event->name) == 0) {
    str = write_event_messagenew(dtxn, msg, event);
  } else if (strcmp(push_notification_event_messageappend.name, (*event)->event->event->name) == 0) {
    str = write_event_messageappend(dtxn, msg, event);
  } else {
    str = write_msg_prefix(dtxn, event_name, msg);
    str_append(str, "}");
  }

  return str;
}
