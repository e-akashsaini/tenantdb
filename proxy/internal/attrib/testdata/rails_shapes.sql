BEGIN
COMMIT
INSERT INTO "active_storage_attachments" ("name", "uuid", "record_type", "record_id", "blob_id", "created_at") VALUES ($1, $1, $1, $1, $1, $1) RETURNING "id"
INSERT INTO "active_storage_blobs" ("key", "filename", "content_type", "metadata", "service_name", "byte_size", "checksum", "created_at", "uuid") VALUES ($1, $1, $1, $1, $1, $1, $1, $1, $1) RETURNING "id"
INSERT INTO "completed_documents" ("submitter_id", "sha1", "created_at", "updated_at") VALUES ($1, $1, $1, $1) RETURNING "id"
INSERT INTO "completed_submitters" ("submitter_id", "submission_id", "account_id", "template_id", "source", "sms_count", "completed_at", "created_at", "updated_at", "verification_method", "is_first") VALUES ($1, $1, $1, $1, $1, $1, $1, $1, $1, $1, $1) RETURNING "id"
INSERT INTO "lock_events" ("key", "event_name", "created_at", "updated_at") VALUES ($1, $1, $1, $1) RETURNING "id"
INSERT INTO "search_entries" ("record_type", "record_id", "account_id", "tsvector", "created_at", "updated_at", "ngram") VALUES ($1, $1, $1, $1, $1, $1, $1) RETURNING "id"
INSERT INTO "submission_events" ("submission_id", "submitter_id", "data", "event_type", "event_timestamp", "created_at", "updated_at", "account_id") VALUES ($1, $1, $1, $1, $1, $1, $1, $1) RETURNING "id"
INSERT INTO "submissions" ("template_id", "created_by_user_id", "archived_at", "created_at", "updated_at", "template_fields", "template_schema", "template_submitters", "source", "submitters_order", "slug", "preferences", "account_id", "expire_at", "name", "variables_schema", "variables", "completed_at") VALUES ($1, $1, $1, $1, $1, $1, $1, $1, $1, $1, $1, $1, $1, $1, $1, $1, $1, $1) RETURNING "id"
INSERT INTO "submitters" ("submission_id", "uuid", "email", "slug", "values", "ua", "ip", "sent_at", "opened_at", "completed_at", "created_at", "updated_at", "name", "phone", "external_id", "preferences", "metadata", "account_id", "declined_at", "timezone") VALUES ($1, $1, $1, $1, $1, $1, $1, $1, $1, $1, $1, $1, $1, $1, $1, $1, $1, $1, $1, $1) RETURNING "id"
INSERT INTO "templates" ("slug", "name", "schema", "fields", "submitters", "author_id", "account_id", "archived_at", "created_at", "updated_at", "source", "folder_id", "external_id", "preferences", "shared_link", "variables_schema") VALUES ($1, $1, $1, $1, $1, $1, $1, $1, $1, $1, $1, $1, $1, $1, $1, $1) RETURNING "id"
ROLLBACK
SELECT "access_tokens".* FROM "access_tokens" WHERE "access_tokens"."user_id" = $1 LIMIT $1
SELECT "account_accesses".* FROM "account_accesses" WHERE "account_accesses"."account_id" = $1
SELECT "account_configs".* FROM "account_configs" WHERE "account_configs"."account_id" = $1 AND "account_configs"."key" = $1 LIMIT $1
SELECT "account_configs".* FROM "account_configs" WHERE "account_configs"."account_id" = $1 AND "account_configs"."key" = $1 ORDER BY "account_configs"."id" ASC LIMIT $1
SELECT "account_configs".* FROM "account_configs" WHERE "account_configs"."account_id" = $1 AND "account_configs"."key" IN ($1, $1)
SELECT "account_configs".* FROM "account_configs" WHERE "account_configs"."account_id" = $1 AND "account_configs"."key" IN ($1, $1, $1, $1, $1, $1)
SELECT "account_configs".* FROM "account_configs" WHERE "account_configs"."account_id" = $1 AND "account_configs"."key" IN ($1, $1, $1, $1, $1, $1, $1, $1)
SELECT "account_configs".* FROM "account_configs" WHERE "account_configs"."account_id" = $1 AND "account_configs"."key" IN ($1, $1, $1, $1, $1, $1, $1, $1, $1, $1, $1, $1, $1, $1, $1, $1, $1, $1)
SELECT "account_configs".* FROM "account_configs" WHERE "account_configs"."key" = $1 ORDER BY "account_configs"."id" ASC LIMIT $1
SELECT "account_linked_accounts".* FROM "account_linked_accounts" WHERE "account_linked_accounts"."linked_account_id" = $1 LIMIT $1
SELECT "accounts".* FROM "accounts" ORDER BY "accounts"."id" ASC LIMIT $1
SELECT "accounts".* FROM "accounts" WHERE "accounts"."id" = $1
SELECT "accounts".* FROM "accounts" WHERE "accounts"."id" = $1 LIMIT $1
SELECT "active_storage_attachments".* FROM "active_storage_attachments" INNER JOIN "active_storage_blobs" "blob" ON "blob"."id" = "active_storage_attachments"."blob_id" WHERE "blob"."filename" IN ($1, $1) AND "active_storage_attachments"."record_id" = $1 AND "active_storage_attachments"."record_type" = $1 AND "active_storage_attachments"."name" = $1
SELECT "active_storage_attachments".* FROM "active_storage_attachments" INNER JOIN "active_storage_blobs" "blob" ON "blob"."id" = "active_storage_attachments"."blob_id" WHERE "blob"."filename" IN ($1, $1) AND "active_storage_attachments"."record_id" IN ($1, $1, $1, $1, $1) AND "active_storage_attachments"."record_type" = $1 AND "active_storage_attachments"."name" = $1
SELECT "active_storage_attachments".* FROM "active_storage_attachments" INNER JOIN "active_storage_blobs" "blob" ON "blob"."id" = "active_storage_attachments"."blob_id" WHERE "blob"."filename" IN ($1, $1) AND "active_storage_attachments"."record_id" IN ($1, $1, $1, $1, $1, $1, $1, $1) AND "active_storage_attachments"."record_type" = $1 AND "active_storage_attachments"."name" = $1
SELECT "active_storage_attachments".* FROM "active_storage_attachments" INNER JOIN "active_storage_blobs" "blob" ON "blob"."id" = "active_storage_attachments"."blob_id" WHERE "blob"."filename" IN ($1, $1) AND "active_storage_attachments"."record_id" IN ($1, $1, $1, $1, $1, $1, $1, $1, $1, $1) AND "active_storage_attachments"."record_type" = $1 AND "active_storage_attachments"."name" = $1
SELECT "active_storage_attachments".* FROM "active_storage_attachments" INNER JOIN "templates" ON "active_storage_attachments"."record_id" = "templates"."id" WHERE "templates"."id" = $1 AND "active_storage_attachments"."record_type" = $1 AND "active_storage_attachments"."name" = $1 AND "active_storage_attachments"."uuid" = $1
SELECT "active_storage_attachments".* FROM "active_storage_attachments" WHERE "active_storage_attachments"."blob_id" = $1
SELECT "active_storage_attachments".* FROM "active_storage_attachments" WHERE "active_storage_attachments"."name" = $1 AND "active_storage_attachments"."record_type" = $1 AND "active_storage_attachments"."record_id" = $1
SELECT "active_storage_attachments".* FROM "active_storage_attachments" WHERE "active_storage_attachments"."record_id" = $1 AND "active_storage_attachments"."record_type" = $1 AND "active_storage_attachments"."name" = $1
SELECT "active_storage_attachments".* FROM "active_storage_attachments" WHERE "active_storage_attachments"."record_id" = $1 AND "active_storage_attachments"."record_type" = $1 AND "active_storage_attachments"."name" = $1 LIMIT $1
SELECT "active_storage_attachments".* FROM "active_storage_attachments" WHERE "active_storage_attachments"."record_id" = $1 AND "active_storage_attachments"."record_type" = $1 AND "active_storage_attachments"."uuid" = $1
SELECT "active_storage_attachments".* FROM "active_storage_attachments" WHERE "active_storage_attachments"."record_id" IN ($1, $1, $1, $1, $1) AND "active_storage_attachments"."record_type" = $1 AND "active_storage_attachments"."name" = $1 AND "active_storage_attachments"."uuid" IN ($1, $1, $1, $1, $1)
SELECT "active_storage_attachments".* FROM "active_storage_attachments" WHERE "active_storage_attachments"."record_id" IN ($1, $1, $1, $1, $1, $1, $1, $1) AND "active_storage_attachments"."record_type" = $1 AND "active_storage_attachments"."name" = $1 AND "active_storage_attachments"."uuid" IN ($1, $1, $1, $1, $1, $1, $1, $1)
SELECT "active_storage_attachments".* FROM "active_storage_attachments" WHERE "active_storage_attachments"."record_id" IN ($1, $1, $1, $1, $1, $1, $1, $1, $1, $1) AND "active_storage_attachments"."record_type" = $1 AND "active_storage_attachments"."name" = $1 AND "active_storage_attachments"."uuid" IN ($1, $1, $1, $1, $1, $1, $1, $1, $1, $1)
SELECT "active_storage_attachments".* FROM "active_storage_attachments" WHERE "active_storage_attachments"."record_type" = $1 AND "active_storage_attachments"."name" = $1 AND "active_storage_attachments"."record_id" = $1
SELECT "active_storage_attachments".* FROM "active_storage_attachments" WHERE "active_storage_attachments"."record_type" = $1 AND "active_storage_attachments"."name" = $1 AND "active_storage_attachments"."record_id" IN ($1, $1, $1, $1, $1)
SELECT "active_storage_attachments".* FROM "active_storage_attachments" WHERE "active_storage_attachments"."record_type" = $1 AND "active_storage_attachments"."name" = $1 AND "active_storage_attachments"."record_id" IN ($1, $1, $1, $1, $1, $1, $1, $1)
SELECT "active_storage_attachments".* FROM "active_storage_attachments" WHERE "active_storage_attachments"."record_type" = $1 AND "active_storage_attachments"."name" = $1 AND "active_storage_attachments"."record_id" IN ($1, $1, $1, $1, $1, $1, $1, $1, $1, $1)
SELECT "active_storage_attachments".* FROM "active_storage_attachments" WHERE "active_storage_attachments"."record_type" = $1 AND "active_storage_attachments"."record_id" IN (SELECT "submitters"."id" FROM "submitters" WHERE "submitters"."submission_id" = $1) AND "active_storage_attachments"."name" = $1
SELECT "active_storage_attachments".* FROM "active_storage_attachments" WHERE "active_storage_attachments"."record_type" = $1 AND "active_storage_attachments"."uuid" = $1 AND "active_storage_attachments"."record_id" = $1
SELECT "active_storage_blobs".* FROM "active_storage_blobs" WHERE "active_storage_blobs"."id" = $1
SELECT "active_storage_blobs".* FROM "active_storage_blobs" WHERE "active_storage_blobs"."id" = $1 LIMIT $1
SELECT "active_storage_blobs".* FROM "active_storage_blobs" WHERE "active_storage_blobs"."id" IN ($1, $1)
SELECT "active_storage_blobs".* FROM "active_storage_blobs" WHERE "active_storage_blobs"."id" IN ($1, $1, $1)
SELECT "active_storage_blobs".* FROM "active_storage_blobs" WHERE "active_storage_blobs"."id" IN ($1, $1, $1, $1)
SELECT "active_storage_blobs".* FROM "active_storage_blobs" WHERE "active_storage_blobs"."id" IN ($1, $1, $1, $1, $1)
SELECT "active_storage_blobs".* FROM "active_storage_blobs" WHERE "active_storage_blobs"."id" IN ($1, $1, $1, $1, $1, $1)
SELECT "active_storage_blobs".* FROM "active_storage_blobs" WHERE "active_storage_blobs"."id" IN ($1, $1, $1, $1, $1, $1, $1, $1)
SELECT "active_storage_blobs".* FROM "active_storage_blobs" WHERE "active_storage_blobs"."id" IN ($1, $1, $1, $1, $1, $1, $1, $1, $1)
SELECT "active_storage_blobs".* FROM "active_storage_blobs" WHERE "active_storage_blobs"."id" IN ($1, $1, $1, $1, $1, $1, $1, $1, $1, $1)
SELECT "completed_documents".* FROM "completed_documents" WHERE "completed_documents"."sha1" = $1 AND "completed_documents"."submitter_id" = $1 LIMIT $1
SELECT "completed_submitters".* FROM "completed_submitters" WHERE "completed_submitters"."submitter_id" = $1 LIMIT $1
SELECT "encrypted_configs".* FROM "encrypted_configs" WHERE "encrypted_configs"."account_id" = $1 AND "encrypted_configs"."key" = $1 LIMIT $1
SELECT "encrypted_configs".* FROM "encrypted_configs" WHERE "encrypted_configs"."key" = $1 LIMIT $1
SELECT "lock_events".* FROM "lock_events" WHERE "lock_events"."key" = $1 ORDER BY "lock_events"."id" ASC
SELECT "lock_events".* FROM "lock_events" WHERE "lock_events"."key" = $1 ORDER BY "lock_events"."id" DESC LIMIT $1
SELECT "schema_migrations"."version" FROM "schema_migrations" ORDER BY "schema_migrations"."version" ASC
SELECT "search_entries".* FROM "search_entries" WHERE "search_entries"."record_id" = $1 AND "search_entries"."record_type" = $1 LIMIT $1
SELECT "search_entries".* FROM "search_entries" WHERE "search_entries"."record_type" = $1 AND "search_entries"."record_id" = $1 LIMIT $1
SELECT "submission_events".* FROM "submission_events" WHERE "submission_events"."event_type" = $1 AND "submission_events"."submitter_id" = $1
SELECT "submission_events".* FROM "submission_events" WHERE "submission_events"."event_type" = $1 AND "submission_events"."submitter_id" IN ($1, $1, $1, $1, $1, $1)
SELECT "submission_events".* FROM "submission_events" WHERE "submission_events"."event_type" = $1 AND "submission_events"."submitter_id" IN ($1, $1, $1, $1, $1, $1, $1, $1)
SELECT "submission_events".* FROM "submission_events" WHERE "submission_events"."event_type" = $1 AND "submission_events"."submitter_id" IN ($1, $1, $1, $1, $1, $1, $1, $1, $1, $1)
SELECT "submission_events".* FROM "submission_events" WHERE "submission_events"."submission_id" = $1
SELECT "submission_events".* FROM "submission_events" WHERE "submission_events"."submitter_id" = $1
SELECT "submission_events".* FROM "submission_events" WHERE "submission_events"."submitter_id" = $1 AND "submission_events"."event_type" IN ($1, $1, $1, $1)
SELECT "submission_events".* FROM "submission_events" WHERE "submission_events"."submitter_id" IN ($1, $1, $1, $1, $1, $1, $1, $1)
SELECT "submission_events".* FROM "submission_events" WHERE "submission_events"."submitter_id" IN ($1, $1, $1, $1, $1, $1, $1, $1, $1, $1)
SELECT "submissions"."id", "submissions"."name", "submissions"."created_by_user_id", "submissions"."account_id", "submissions"."completed_at", "submissions"."created_at", "submissions"."archived_at", "submissions"."expire_at", "submissions"."template_id", "submissions"."template_submitters" FROM "submissions" LEFT OUTER JOIN "templates" ON "templates"."id" = "submissions"."template_id" WHERE "submissions"."account_id" = $1 AND "submissions"."archived_at" IS NULL AND "templates"."archived_at" IS NULL ORDER BY "submissions"."id" DESC LIMIT $1 OFFSET $1
SELECT "submissions"."id", "submissions"."name", "submissions"."created_by_user_id", "submissions"."account_id", "submissions"."completed_at", "submissions"."created_at", "submissions"."archived_at", "submissions"."expire_at", "submissions"."template_id", "submissions"."template_submitters" FROM "submissions" WHERE "submissions"."template_id" = $1 AND "submissions"."account_id" = $1 AND "submissions"."archived_at" IS NULL ORDER BY "submissions"."id" DESC LIMIT $1 OFFSET $1
SELECT "submissions"."id", "submissions"."name", "submissions"."slug", "submissions"."source", "submissions"."submitters_order", "submissions"."expire_at", "submissions"."completed_at", "submissions"."created_at", "submissions"."updated_at", "submissions"."archived_at", "submissions"."variables", "submissions"."template_id", "submissions"."template_submitters", "submissions"."created_by_user_id" FROM "submissions" WHERE "submissions"."account_id" = $1 AND "submissions"."completed_at" IS NOT NULL ORDER BY "submissions"."id" DESC LIMIT $1
SELECT "submissions"."id", "submissions"."name", "submissions"."slug", "submissions"."source", "submissions"."submitters_order", "submissions"."expire_at", "submissions"."completed_at", "submissions"."created_at", "submissions"."updated_at", "submissions"."archived_at", "submissions"."variables", "submissions"."template_id", "submissions"."template_submitters", "submissions"."created_by_user_id" FROM "submissions" WHERE "submissions"."account_id" = $1 ORDER BY "submissions"."id" DESC LIMIT $1
SELECT "submissions".* FROM "submissions" WHERE "submissions"."id" = $1
SELECT "submissions".* FROM "submissions" WHERE "submissions"."id" = $1 LIMIT $1
SELECT "submissions".* FROM "submissions" WHERE "submissions"."id" IN ($1, $1, $1, $1, $1, $1, $1, $1)
SELECT "submissions".* FROM "submissions" WHERE "submissions"."id" IN ($1, $1, $1, $1, $1, $1, $1, $1, $1, $1)
SELECT "submitter_versions".* FROM "submitter_versions" WHERE "submitter_versions"."submitter_id" = $1
SELECT "submitters".* FROM "submitters" WHERE "submitters"."account_id" = $1 AND "submitters"."completed_at" IS NOT NULL ORDER BY "submitters"."id" DESC LIMIT $1
SELECT "submitters".* FROM "submitters" WHERE "submitters"."account_id" = $1 ORDER BY "submitters"."id" DESC LIMIT $1
SELECT "submitters".* FROM "submitters" WHERE "submitters"."id" = $1 LIMIT $1
SELECT "submitters".* FROM "submitters" WHERE "submitters"."slug" = $1 LIMIT $1
SELECT "submitters".* FROM "submitters" WHERE "submitters"."submission_id" = $1
SELECT "submitters".* FROM "submitters" WHERE "submitters"."submission_id" IN ($1, $1, $1, $1, $1)
SELECT "submitters".* FROM "submitters" WHERE "submitters"."submission_id" IN ($1, $1, $1, $1, $1, $1)
SELECT "submitters".* FROM "submitters" WHERE "submitters"."submission_id" IN ($1, $1, $1, $1, $1, $1, $1, $1)
SELECT "submitters".* FROM "submitters" WHERE "submitters"."submission_id" IN ($1, $1, $1, $1, $1, $1, $1, $1, $1, $1)
SELECT "template_accesses".* FROM "template_accesses" WHERE "template_accesses"."template_id" = $1
SELECT "template_accesses".* FROM "template_accesses" WHERE "template_accesses"."template_id" IN ($1, $1, $1, $1, $1, $1)
SELECT "template_accesses".* FROM "template_accesses" WHERE "template_accesses"."template_id" IN ($1, $1, $1, $1, $1, $1, $1, $1)
SELECT "template_accesses".* FROM "template_accesses" WHERE "template_accesses"."template_id" IN ($1, $1, $1, $1, $1, $1, $1, $1, $1, $1)
SELECT "template_accesses".* FROM "template_accesses" WHERE "template_accesses"."template_id" IN ($1, $1, $1, $1, $1, $1, $1, $1, $1, $1, $1, $1)
SELECT "template_folders".* FROM "template_folders" WHERE "template_folders"."account_id" = $1 AND "template_folders"."name" = $1 LIMIT $1
SELECT "template_folders".* FROM "template_folders" WHERE "template_folders"."account_id" = $1 AND "template_folders"."parent_folder_id" IS NULL AND (EXISTS (SELECT 1 FROM "templates" WHERE "templates"."account_id" = $1 AND "templates"."archived_at" IS NULL AND "template_folders"."id" = "templates"."folder_id" LIMIT $1) OR EXISTS (SELECT 1 FROM "template_folders" "subfolders" WHERE "subfolders"."parent_folder_id" = "template_folders"."id" AND EXISTS (SELECT "templates".* FROM "templates" WHERE "templates"."account_id" = $1 AND "templates"."archived_at" IS NULL AND "templates"."folder_id" = "subfolders"."id") LIMIT $1)) AND "template_folders"."id" != $1 ORDER BY "template_folders"."id" DESC LIMIT $1 OFFSET $1
SELECT "template_folders".* FROM "template_folders" WHERE "template_folders"."id" = $1
SELECT "template_folders".* FROM "template_folders" WHERE "template_folders"."id" = $1 LIMIT $1
SELECT "templates"."id", "templates"."name", "templates"."author_id", "templates"."account_id", "templates"."created_at", "templates"."archived_at", "templates"."folder_id" FROM "templates" WHERE "templates"."account_id" = $1 AND "templates"."archived_at" IS NULL AND "templates"."folder_id" = $1 ORDER BY "templates"."id" DESC LIMIT $1 OFFSET $1
SELECT "templates"."id", "templates"."name", "templates"."author_id", "templates"."account_id", "templates"."created_at", "templates"."archived_at", "templates"."folder_id" FROM "templates" WHERE "templates"."id" IN ($1, $1, $1, $1, $1, $1)
SELECT "templates"."id", "templates"."name", "templates"."author_id", "templates"."account_id", "templates"."created_at", "templates"."archived_at", "templates"."folder_id" FROM "templates" WHERE "templates"."id" IN ($1, $1, $1, $1, $1, $1, $1, $1)
SELECT "templates"."id", "templates"."name", "templates"."author_id", "templates"."account_id", "templates"."created_at", "templates"."archived_at", "templates"."folder_id" FROM "templates" WHERE "templates"."id" IN ($1, $1, $1, $1, $1, $1, $1, $1, $1, $1)
SELECT "templates"."id", "templates"."name", "templates"."external_id", "templates"."created_at", "templates"."updated_at", "templates"."folder_id", "templates"."submitters" FROM "templates" WHERE "templates"."id" IN ($1, $1, $1, $1, $1)
SELECT "templates"."id", "templates"."name", "templates"."external_id", "templates"."created_at", "templates"."updated_at", "templates"."folder_id", "templates"."submitters" FROM "templates" WHERE "templates"."id" IN ($1, $1, $1, $1, $1, $1, $1, $1)
SELECT "templates"."id", "templates"."name", "templates"."external_id", "templates"."created_at", "templates"."updated_at", "templates"."folder_id", "templates"."submitters" FROM "templates" WHERE "templates"."id" IN ($1, $1, $1, $1, $1, $1, $1, $1, $1, $1)
SELECT "templates".* FROM "templates" INNER JOIN "submissions" ON "templates"."id" = "submissions"."template_id" WHERE "submissions"."id" = $1 LIMIT $1
SELECT "templates".* FROM "templates" WHERE "templates"."account_id" = $1 AND "templates"."archived_at" IS NULL ORDER BY "templates"."id" DESC LIMIT $1
SELECT "templates".* FROM "templates" WHERE "templates"."account_id" = $1 ORDER BY "templates"."id" DESC LIMIT $1
SELECT "templates".* FROM "templates" WHERE "templates"."id" = $1
SELECT "templates".* FROM "templates" WHERE "templates"."id" = $1 LIMIT $1
SELECT "templates".* FROM "templates" WHERE "templates"."id" IN ($1, $1, $1, $1, $1, $1, $1, $1)
SELECT "templates".* FROM "templates" WHERE "templates"."id" IN ($1, $1, $1, $1, $1, $1, $1, $1, $1, $1)
SELECT "user_configs".* FROM "user_configs" WHERE "user_configs"."user_id" = $1 AND "user_configs"."key" = $1 LIMIT $1
SELECT "users".* FROM "users" INNER JOIN "access_tokens" "access_token" ON "access_token"."user_id" = "users"."id" WHERE "users"."archived_at" IS NULL AND "access_token"."sha1" = $1 LIMIT $1
SELECT "users".* FROM "users" ORDER BY "users"."id" ASC LIMIT $1
SELECT "users".* FROM "users" WHERE "users"."account_id" = $1 AND "users"."archived_at" IS NULL AND "users"."role" != $1 AND "users"."account_id" = $1 ORDER BY "users"."id" DESC LIMIT $1 OFFSET $1
SELECT "users".* FROM "users" WHERE "users"."email" = $1 ORDER BY "users"."id" ASC LIMIT $1
SELECT "users".* FROM "users" WHERE "users"."id" = $1
SELECT "users".* FROM "users" WHERE "users"."id" = $1 LIMIT $1
SELECT "users".* FROM "users" WHERE "users"."id" = $1 ORDER BY "users"."id" ASC LIMIT $1
SELECT "webhook_urls".* FROM "webhook_urls" WHERE "webhook_urls"."account_id" = $1 AND "webhook_urls"."events" ILIKE 'x'
SELECT "webhook_urls".* FROM "webhook_urls" WHERE "webhook_urls"."account_id" = $1 AND ("webhook_urls"."events" ILIKE 'x' OR "webhook_urls"."events" ILIKE 'x')
SELECT "webhook_urls".* FROM "webhook_urls" WHERE "webhook_urls"."account_id" = $1 ORDER BY "webhook_urls"."id" DESC LIMIT $1
SELECT * FROM "ar_internal_metadata" WHERE "ar_internal_metadata"."key" = $1 ORDER BY "ar_internal_metadata"."key" ASC LIMIT 1
SELECT 1 AS one FROM "account_configs" WHERE "account_configs"."account_id" = $1 AND "account_configs"."key" = $1 AND "account_configs"."value" = $1 LIMIT $1
SELECT 1 AS one FROM "account_configs" WHERE "account_configs"."value" = $1 AND "account_configs"."account_id" = $1 AND "account_configs"."key" = $1 LIMIT $1
SELECT 1 AS one FROM "completed_submitters" WHERE "completed_submitters"."submission_id" = $1 AND "completed_submitters"."is_first" = $1 LIMIT $1
SELECT 1 AS one FROM "encrypted_configs" WHERE "encrypted_configs"."key" = $1 LIMIT $1
SELECT 1 AS one FROM "lock_events" WHERE "lock_events"."key" = $1 AND "lock_events"."event_name" = $1 LIMIT $1
SELECT 1 AS one FROM "submissions" WHERE "submissions"."account_id" = $1 AND "submissions"."archived_at" IS NOT NULL LIMIT $1
SELECT 1 AS one FROM "submissions" WHERE "submissions"."template_id" = $1 AND "submissions"."archived_at" IS NOT NULL LIMIT $1
SELECT 1 AS one FROM "template_versions" WHERE "template_versions"."template_id" = $1 LIMIT $1
SELECT 1 AS one FROM "templates" WHERE "templates"."account_id" = $1 AND "templates"."archived_at" IS NOT NULL LIMIT $1
SELECT 1 AS one FROM "templates" WHERE "templates"."account_id" = $1 AND "templates"."archived_at" IS NULL AND "templates"."folder_id" = $1 LIMIT $1
SELECT 1 AS one FROM "templates" WHERE "templates"."account_id" = $1 AND EXISTS (SELECT "submissions".* FROM "submissions" WHERE "submissions"."template_id" = "templates"."id") AND "templates"."archived_at" IS NOT NULL LIMIT $1
SELECT 1 AS one FROM "users" LIMIT $1
SELECT 1 AS one FROM "users" WHERE "users"."account_id" = $1 AND "users"."archived_at" IS NOT NULL AND "users"."role" != $1 LIMIT $1
SELECT 1 AS one FROM "users" WHERE "users"."account_id" = $1 AND "users"."archived_at" IS NULL AND "users"."role" = $1 LIMIT $1
SELECT 1 AS one FROM "users" WHERE "users"."account_id" = $1 AND "users"."id" = $1 LIMIT $1
SELECT 1 AS one FROM "users" WHERE "users"."email" = $1 AND "users"."otp_required_for_login" = $1 LIMIT $1
SELECT COUNT(*) FROM "submissions" LEFT OUTER JOIN "templates" ON "templates"."id" = "submissions"."template_id" WHERE "submissions"."account_id" = $1 AND "submissions"."archived_at" IS NULL AND "templates"."archived_at" IS NULL
SELECT COUNT(*) FROM "submissions" WHERE "submissions"."template_id" = $1 AND "submissions"."account_id" = $1 AND "submissions"."archived_at" IS NULL
SELECT COUNT(*) FROM "template_folders" WHERE "template_folders"."account_id" = $1 AND "template_folders"."parent_folder_id" IS NULL AND (EXISTS (SELECT 1 FROM "templates" WHERE "templates"."account_id" = $1 AND "templates"."archived_at" IS NULL AND "template_folders"."id" = "templates"."folder_id" LIMIT $1) OR EXISTS (SELECT 1 FROM "template_folders" "subfolders" WHERE "subfolders"."parent_folder_id" = "template_folders"."id" AND EXISTS (SELECT "templates".* FROM "templates" WHERE "templates"."account_id" = $1 AND "templates"."archived_at" IS NULL AND "templates"."folder_id" = "subfolders"."id") LIMIT $1)) AND "template_folders"."id" != $1
SELECT COUNT(*) FROM "templates" WHERE "templates"."account_id" = $1 AND "templates"."archived_at" IS NULL AND "templates"."folder_id" = $1
SELECT COUNT(*) FROM "users" WHERE "users"."account_id" = $1 AND "users"."archived_at" IS NULL AND "users"."role" != $1 AND "users"."account_id" = $1
SELECT COUNT(*) FROM "webhook_urls" WHERE "webhook_urls"."account_id" = $1
SELECT COUNT(*) FROM (SELECT 1 AS one FROM "template_folders" WHERE "template_folders"."account_id" = $1 AND "template_folders"."parent_folder_id" IS NULL AND (EXISTS (SELECT 1 FROM "templates" WHERE "templates"."account_id" = $1 AND "templates"."archived_at" IS NULL AND "template_folders"."id" = "templates"."folder_id" LIMIT $1) OR EXISTS (SELECT 1 FROM "template_folders" "subfolders" WHERE "subfolders"."parent_folder_id" = "template_folders"."id" AND EXISTS (SELECT "templates".* FROM "templates" WHERE "templates"."account_id" = $1 AND "templates"."archived_at" IS NULL AND "templates"."folder_id" = "subfolders"."id") LIMIT $1)) AND "template_folders"."id" != $1 LIMIT $1 OFFSET $1) subquery_for_count
SELECT COUNT(*) FROM (SELECT 1 AS one FROM "templates" WHERE "templates"."account_id" = $1 AND "templates"."archived_at" IS NULL AND "templates"."folder_id" = $1 LIMIT $1 OFFSET $1) subquery_for_count
SELECT MAX("submitters"."completed_at") FROM "submitters" WHERE "submitters"."submission_id" = $1
SELECT a.attname
SELECT a.attname, format_type(a.atttypid, a.atttypmod),
SELECT c.relname FROM pg_class c LEFT JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = ANY (current_schemas(false)) AND c.relkind IN ('x','x','x','x','x')
SELECT c.relname FROM pg_class c LEFT JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = ANY (current_schemas(false)) AND c.relname = 'x' AND c.relkind IN ('x','x')
SELECT c.relname FROM pg_class c LEFT JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = ANY (current_schemas(false)) AND c.relname = 'x' AND c.relkind IN ('x','x','x','x','x')
SELECT current_database()
SELECT pg_advisory_unlock(1)
SELECT pg_try_advisory_lock(1)
SELECT setweight(to_tsvector('x'), 'x') || setweight(to_tsvector('x'), 'x') || setweight(to_tsvector('x'), 'x') || setweight(to_tsvector('x'), 'x') as tsvector, setweight(to_tsvector('x', 'x'), 'x') || setweight(to_tsvector('x', 'x'), 'x') || setweight(to_tsvector('x', 'x'), 'x') as ngram
SELECT t.oid, t.typname
SELECT t.oid, t.typname, t.typelem, t.typdelim, t.typinput, r.rngsubtype, t.typtype, t.typbasetype
SELECT to_tsvector('x'), to_tsvector('x', 'x')
SET SESSION timezone TO 'x'
SET client_min_messages TO 'x'
SET intervalstyle = iso_1
SET standard_conforming_strings = on
SHOW max_identifier_length
SHOW search_path
UPDATE "active_storage_blobs" SET "metadata" = $1 WHERE "active_storage_blobs"."id" = $1
UPDATE "search_entries" SET "updated_at" = $1, "ngram" = $1 WHERE "search_entries"."id" = $1
UPDATE "submissions" SET "completed_at" = (SELECT MAX("submitters"."completed_at") FROM "submitters" WHERE "submitters"."submission_id" = "submissions"."id") WHERE "submissions"."id" = $1 AND "submissions"."completed_at" IS NULL AND NOT (EXISTS (SELECT 1 FROM "submitters" WHERE "submitters"."submission_id" = $1 AND "submitters"."completed_at" IS NULL LIMIT $1))
UPDATE "submissions" SET "updated_at" = $1 WHERE "submissions"."id" = $1
UPDATE "submissions" SET "updated_at" = $1, "template_fields" = $1, "template_schema" = $1 WHERE "submissions"."id" = $1
UPDATE "submitters" SET "completed_at" = $1, "updated_at" = $1 WHERE "submitters"."id" = $1
UPDATE "templates" SET "fields" = $1, "updated_at" = $1 WHERE "templates"."id" = $1
UPDATE "templates" SET "schema" = $1, "updated_at" = $1 WHERE "templates"."id" = $1
UPDATE "templates" SET "updated_at" = $1 WHERE "templates"."id" = $1
UPDATE "users" SET "sign_in_count" = $1, "current_sign_in_at" = $1, "updated_at" = $1 WHERE "users"."id" = $1
set client_encoding to 'x'
