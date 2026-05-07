-- +goose Up
-- +goose StatementBegin
-- Initial placeholder migration.
-- Replace with your schema when cloning the template.
--
-- goose conventions:
--   - one file per migration, filename prefixed with UTC timestamp
--   - `-- +goose Up` / `-- +goose Down` sections required
--   - use `-- +goose StatementBegin/End` when a statement contains semicolons
--     (e.g. plpgsql functions) so goose doesn't split on `;`

CREATE TABLE IF NOT EXISTS example (
    id          BIGSERIAL PRIMARY KEY,
    name        TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS example;
-- +goose StatementEnd
