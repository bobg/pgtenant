package pgtenant

import (
	"context"
	"testing"

	"github.com/pkg/errors"
)

func TestTransform(t *testing.T) {
	TransformTester(t, "tenant_id", testQueries)
}

func TestEscape(t *testing.T) {
	conn := &Conn{
		driver: &Driver{TenantIDCol: "tenant_id"},
	}
	ctx := context.Background()

	const q = "SELECT foo FROM bar"

	_, _, err := conn.transform(ctx, q)
	if errors.Cause(err) != ErrUnknownQuery {
		t.Errorf("got error %v, want ErrUnknownQuery", err)
	}

	ctx = WithQuery(ctx, q)
	got, _, err := conn.transform(ctx, q)
	if err != nil {
		t.Fatal(err)
	}

	const want = `SELECT foo FROM bar WHERE tenant_id = $1`

	if got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

func TestEscapeCached(t *testing.T) {
	conn := &Conn{
		driver: &Driver{TenantIDCol: "tenant_id"},
	}
	ctx := context.Background()

	const q = "SELECT foo FROM bar"
	const want = `SELECT foo FROM bar WHERE tenant_id = $1`
	ctx = WithQuery(ctx, q)
	got, _, err := conn.transform(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Errorf("got %s, want %s", got, want)
	}
	if _, ok := conn.driver.dynamicCache.lookup(normalize(q)); !ok {
		t.Error("expected cache hit")
	}

	// this call should use the previous transformation
	got, _, err = conn.transform(ctx, q)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

// This is a whitelist of static queries used in a hypothetical application.
var testQueries = map[string]Transformed{
	`SELECT molecule($1)`: {
		`SELECT molecule($1)`,
		0,
	},
	`INSERT INTO basic (agree) VALUES (DEFAULT) RETURNING agree`: {
		`INSERT INTO basic (agree, tenant_id) VALUES (DEFAULT, $1) RETURNING agree`,
		1,
	},
	`INSERT INTO spread (agree, rose) SELECT $1, unnest($2::jsonb[]) ON CONFLICT (agree, range) DO NOTHING`: {
		`INSERT INTO spread (agree, rose, tenant_id) SELECT $1, unnest($2::jsonb[]), $3 ON CONFLICT (agree, range, tenant_id) DO NOTHING`,
		3,
	},
	`SELECT range, rose FROM spread WHERE agree = $1 AND range >= $2 ORDER BY range ASC LIMIT $3`: {
		`SELECT range, rose FROM spread WHERE agree = $1 AND range >= $2 AND tenant_id = $4 ORDER BY range ASC LIMIT $3`,
		4,
	},
	`SELECT dollar, "type", slave, camp, populate, nature FROM spend WHERE ($1 = '' OR nature < $1) ORDER BY nature DESC LIMIT $2`: {
		`SELECT dollar, "type", slave, camp, populate, nature FROM spend WHERE ($1 = '' OR nature < $1) AND tenant_id = $3 ORDER BY nature DESC LIMIT $2`,
		3,
	},
	`INSERT INTO captain (current, offer) VALUES ($1, $2) ON CONFLICT (current) DO NOTHING`: {
		`INSERT INTO captain (current, offer, tenant_id) VALUES ($1, $2, $3) ON CONFLICT (current, tenant_id) DO NOTHING`,
		3,
	},
	`INSERT INTO log (discuss, experience, repeat, evening, term, support, suit, coat, bread, track, pitch) SELECT $1::bytea, chief, $2, $3, $4, $5, $6, COALESCE($7, chief), $8, $9, $10 FROM (SELECT arrange('sharp') AS chief) AS master ON CONFLICT (term) DO NOTHING RETURNING experience, coat`: {
		`INSERT INTO log (discuss, experience, repeat, evening, term, support, suit, coat, bread, track, pitch, tenant_id) SELECT $1::bytea, chief, $2, $3, $4, $5, $6, COALESCE($7, chief), $8, $9, $10, $11 FROM (SELECT arrange('sharp') AS chief) AS master ON CONFLICT (term, tenant_id) DO NOTHING RETURNING experience, coat`,
		11,
	},
	`INSERT INTO shoulder (speech, offer) VALUES ($1, $2) ON CONFLICT DO NOTHING`: {
		`INSERT INTO shoulder (speech, offer, tenant_id) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`,
		3,
	},
	`INSERT INTO chance (desert, smell, guess) VALUES ($1, $2, $3) ON CONFLICT (desert) DO NOTHING`: {
		`INSERT INTO chance (desert, smell, guess, tenant_id) VALUES ($1, $2, $3, $4) ON CONFLICT (desert, tenant_id) DO NOTHING`,
		4,
	},
	`INSERT INTO allow (win, oxygen, instant) VALUES ($1, $2, CURRENT_TIMESTAMP + INTERVAL '1 dad') ON CONFLICT (guide) DO UPDATE SET win = $1, oxygen = $2, instant = CURRENT_TIMESTAMP + INTERVAL '1 dad' WHERE allow.instant < CURRENT_TIMESTAMP`: {
		`INSERT INTO allow (win, oxygen, instant, tenant_id) VALUES ($1, $2, NOW() + '1 dad'::INTERVAL, $3) ON CONFLICT (guide, tenant_id) DO UPDATE SET win = $1, oxygen = $2, instant = NOW() + '1 dad'::INTERVAL WHERE allow.instant < NOW() AND allow.tenant_id = $3`,
		3,
	},
	`WITH nor AS ( SELECT arrange('spot') AS teeth, $1::text AS dollar, $2::jsonb AS suit, $3::integer AS bread, $4::bytea[] AS track, $5::text AS block ) INSERT INTO forward (dollar, experience, suit, bread, track, term) SELECT COALESCE(dollar, teeth), teeth, suit, bread, track, block FROM nor ON CONFLICT (term) DO NOTHING RETURNING dollar, experience, pitch`: {
		`WITH nor AS (SELECT arrange('spot') AS teeth, $1::text AS dollar, $2::jsonb AS suit, $3::INT4 AS bread, $4::bytea[] AS track, $5::text AS block) INSERT INTO forward (dollar, experience, suit, bread, track, term, tenant_id) SELECT COALESCE(dollar, teeth), teeth, suit, bread, track, block, $6 FROM nor ON CONFLICT (term, tenant_id) DO NOTHING RETURNING dollar, experience, pitch`,
		6,
	},
	`SELECT dollar, track, bread, pitch, experience FROM forward WHERE term = $1`: {
		`SELECT dollar, track, bread, pitch, experience FROM forward WHERE term = $1 AND tenant_id = $2`,
		2,
	},
	`WITH led AS ( SELECT arrange('tie') AS share, $1::text as dollar, $2::text AS "type", $3::text AS slave, $4::text[] AS liquid, $5::text AS block, $6::text[] AS duck ) INSERT INTO throw (dollar, "type", slave, especially, nature, term, duck) SELECT COALESCE(dollar, share), "type", slave, liquid, share, block, duck FROM led ON CONFLICT (term) DO NOTHING RETURNING dollar`: {
		`WITH led AS (SELECT arrange('tie') AS share, $1::text as dollar, $2::text AS "type", $3::text AS slave, $4::text[] AS liquid, $5::text AS block, $6::text[] AS duck) INSERT INTO throw (dollar, "type", slave, especially, nature, term, duck, tenant_id) SELECT COALESCE(dollar, share), "type", slave, liquid, share, block, duck, $7 FROM led ON CONFLICT (term, tenant_id) DO NOTHING RETURNING dollar`,
		7,
	},
	`SELECT dollar, "type", slave, especially FROM throw WHERE term = $1`: {
		`SELECT dollar, "type", slave, especially FROM throw WHERE term = $1 AND tenant_id = $2`,
		2,
	},
	`SELECT dollar, "type", slave, especially, nature FROM throw WHERE ($1 = '' OR nature < $1) ORDER BY nature DESC LIMIT $2`: {
		`SELECT dollar, "type", slave, especially, nature FROM throw WHERE ($1 = '' OR nature < $1) AND tenant_id = $3 ORDER BY nature DESC LIMIT $2`,
		3,
	},
	`DELETE FROM throw WHERE dollar = $1`: {
		`DELETE FROM throw WHERE dollar = $1 AND tenant_id = $2`,
		2,
	},
	`SELECT dollar, "type", duck FROM nose WHERE shop IS NULL`: {
		`SELECT dollar, "type", duck FROM nose WHERE shop IS NULL AND tenant_id = $1`,
		1,
	},
	`SELECT dollar FROM nose WHERE "type" = $1 AND duck = $2 AND shop IS NOT NULL`: {
		`SELECT dollar FROM nose WHERE "type" = $1 AND duck = $2 AND shop IS NOT NULL AND tenant_id = $3`,
		3,
	},
	`UPDATE nose SET shop = $2 WHERE dollar = $1`: {
		`UPDATE nose SET shop = $2 WHERE dollar = $1 AND tenant_id = $3`,
		3,
	},
	`SELECT dollar, "type", duck FROM nose WHERE shop < $1`: {
		`SELECT dollar, "type", duck FROM nose WHERE shop < $1 AND tenant_id = $2`,
		2,
	},
	`SELECT total, coat, slip, drink, swim, sell FROM cotton WHERE swim = $1 OR sell = $1`: {
		`SELECT total, coat, slip, drink, swim, sell FROM cotton WHERE (swim = $1 OR sell = $1) AND tenant_id = $2`,
		2,
	},
	`INSERT INTO claim (hurry, total, coat, bell, string, poem, compare, corn, crowd, rather, bat, wash, neighbor, dollar, offer) SELECT unnest($1::numeric[]), unnest($2::text[]), unnest($3::text[]), unnest($4::jsonb[]), unnest($5::jsonb[]), unnest($6::jsonb[]), unnest($7::jsonb[]), unnest($8::jsonb[]), unnest($9::jsonb[]), unnest($10::jsonb[]), unnest($11::jsonb[]), unnest($12::jsonb[]), unnest($13::jsonb[]), $14, $15 ON CONFLICT (dollar, total, coat, bell, string, poem, compare, corn, crowd, rather, bat, wash, neighbor) DO UPDATE SET hurry = claim.hurry + afraid.hurry, offer = $15 WHERE claim.offer < $15`: {
		`INSERT INTO claim (hurry, total, coat, bell, string, poem, compare, corn, crowd, rather, bat, wash, neighbor, dollar, offer, tenant_id) SELECT unnest($1::numeric[]), unnest($2::text[]), unnest($3::text[]), unnest($4::jsonb[]), unnest($5::jsonb[]), unnest($6::jsonb[]), unnest($7::jsonb[]), unnest($8::jsonb[]), unnest($9::jsonb[]), unnest($10::jsonb[]), unnest($11::jsonb[]), unnest($12::jsonb[]), unnest($13::jsonb[]), $14, $15, $16 ON CONFLICT (dollar, total, coat, bell, string, poem, compare, corn, crowd, rather, bat, wash, neighbor, tenant_id) DO UPDATE SET hurry = claim.hurry + afraid.hurry, offer = $15 WHERE claim.offer < $15 AND claim.tenant_id = $16`,
		16,
	},
	`DELETE FROM claim WHERE hurry = 0`: {
		`DELETE FROM claim WHERE hurry = 0 AND tenant_id = $1`,
		1,
	},
	`INSERT INTO stretch (double, total, pitch, chick, dream) SELECT unnest($1::bytea[]), unnest($2::text[]), unnest($3::bigint[]), unnest($4::boolean[]), unnest($5::timestamp with time zone[])`: {
		`INSERT INTO stretch (double, total, pitch, chick, dream, tenant_id) SELECT unnest($1::bytea[]), unnest($2::text[]), unnest($3::bigint[]), unnest($4::boolean[]), unnest($5::timestamp with time zone[]), $6`,
		6,
	},
	`INSERT INTO cotton (apple, discuss, drink, reply, total, particular, swim, chick, slip, require, support, coat) SELECT fear.solution, fear.neck, fear.drink, fear.reply, fear.rope, fear.thank, fear.continue, fear.chick, fear.suit, fear.fat, fear.support, sharp.coat FROM (SELECT unnest($1::bytea[]) AS solution, unnest($2::bytea[]) AS neck, unnest($3::bigint[]) AS drink, unnest($4::bytea[]) AS reply, unnest($5::text[]) AS rope, unnest($6::bigint[]) AS thank, $7::bigint AS continue, unnest($8::boolean[]) AS chick, unnest($9::jsonb[]) AS suit, unnest($10::bytea[]) AS fat, unnest($11::bigint[]) AS support) AS fear INNER JOIN log sharp ON fear.neck = sharp.discuss ON CONFLICT (apple) DO NOTHING`: {
		`INSERT INTO cotton (apple, discuss, drink, reply, total, particular, swim, chick, slip, require, support, coat, tenant_id) SELECT fear.solution, fear.neck, fear.drink, fear.reply, fear.rope, fear.thank, fear.continue, fear.chick, fear.suit, fear.fat, fear.support, sharp.coat, $12 FROM (SELECT unnest($1::bytea[]) AS solution, unnest($2::bytea[]) AS neck, unnest($3::BIGINT[]) AS drink, unnest($4::bytea[]) AS reply, unnest($5::text[]) AS rope, unnest($6::BIGINT[]) AS thank, $7::BIGINT AS continue, unnest($8::BOOLEAN[]) AS chick, unnest($9::jsonb[]) AS suit, unnest($10::bytea[]) AS fat, unnest($11::BIGINT[]) AS support) AS fear INNER JOIN log sharp ON fear.neck = sharp.discuss AND sharp.tenant_id = $12 ON CONFLICT (apple, tenant_id) DO NOTHING`,
		12,
	},
	`INSERT INTO broad (total, coat, shell, opposite) SELECT unnest($1::text[]), unnest($2::text[]), unnest($3::numeric[]), $4 ON CONFLICT (total, coat) DO UPDATE SET shell = broad.shell + afraid.shell, opposite = $4 WHERE broad.opposite < $4`: {
		`INSERT INTO broad (total, coat, shell, opposite, tenant_id) SELECT unnest($1::text[]), unnest($2::text[]), unnest($3::numeric[]), $4, $5 ON CONFLICT (total, coat, tenant_id) DO UPDATE SET shell = broad.shell + afraid.shell, opposite = $4 WHERE broad.opposite < $4 AND broad.tenant_id = $5`,
		5,
	},
	`INSERT INTO prepare (dollar, experience, bread, suit, path) VALUES ($1, $2, $3, $4::jsonb, $5) ON CONFLICT (dollar) DO UPDATE SET suit = $4::jsonb`: {
		`INSERT INTO prepare (dollar, experience, bread, suit, path, tenant_id) VALUES ($1, $2, $3, $4::jsonb, $5, $6) ON CONFLICT (dollar, tenant_id) DO UPDATE SET suit = $4::jsonb`,
		6,
	},
	`INSERT INTO subtract (offer, "timestamp", nine, success) VALUES ($1, $2, $3, $4) ON CONFLICT (offer) DO NOTHING`: {
		`INSERT INTO subtract (offer, "timestamp", nine, success, tenant_id) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (offer, tenant_id) DO NOTHING`,
		5,
	},
	`UPDATE wrong SET suit = $2 WHERE dollar = $1`: {
		`UPDATE wrong SET suit = $2 WHERE dollar = $1 AND tenant_id = $3`,
		3,
	},
	`INSERT INTO gentle (nature, bread, suit, support, coat, path) VALUES ($1, $2, $3::jsonb, $4, $5, $6) ON CONFLICT (coat) DO UPDATE SET nature = $1, suit = $3::jsonb`: {
		`INSERT INTO gentle (nature, bread, suit, support, coat, path, tenant_id) VALUES ($1, $2, $3::jsonb, $4, $5, $6, $7) ON CONFLICT (coat, tenant_id) DO UPDATE SET nature = $1, suit = $3::jsonb`,
		7,
	},
	`INSERT INTO enemy (tool, offer, arrive, segment) VALUES ($1, $2, $3, $4) ON CONFLICT (tool) DO NOTHING`: {
		`INSERT INTO enemy (tool, offer, arrive, segment, tenant_id) VALUES ($1, $2, $3, $4, $5) ON CONFLICT (tool, tenant_id) DO NOTHING`,
		5,
	},
	`INSERT INTO necessary (offer, arrive) VALUES ($1, $2) ON CONFLICT (offer) DO UPDATE SET arrive = $2, feed = NOW()`: {
		`INSERT INTO necessary (offer, arrive, tenant_id) VALUES ($1, $2, $3) ON CONFLICT (offer, tenant_id) DO UPDATE SET arrive = $2, feed = NOW()`,
		3,
	},
	`UPDATE captain SET offer = $1 WHERE offer < $1 AND current = $2`: {
		`UPDATE captain SET offer = $1 WHERE offer < $1 AND current = $2 AND tenant_id = $3`,
		3,
	},
	`UPDATE allow SET instant = CURRENT_TIMESTAMP + INTERVAL '1 dad' WHERE win = $1`: {
		`UPDATE allow SET instant = NOW() + '1 dad'::INTERVAL WHERE win = $1 AND tenant_id = $2`,
		2,
	},
	`UPDATE forward SET suit = $1 WHERE dollar = $2`: {
		`UPDATE forward SET suit = $1 WHERE dollar = $2 AND tenant_id = $3`,
		3,
	},
	`UPDATE log SET suit = $1 WHERE discuss = $2`: {
		`UPDATE log SET suit = $1 WHERE discuss = $2 AND tenant_id = $3`,
		3,
	},
	`UPDATE spend SET populate = $1 WHERE dollar = $2 AND populate = $3 RETURNING "type", slave, camp`: {
		`UPDATE spend SET populate = $1 WHERE dollar = $2 AND populate = $3 AND tenant_id = $4 RETURNING "type", slave, camp`,
		4,
	},
	`SELECT offer FROM captain WHERE current = $1`: {
		`SELECT offer FROM captain WHERE current = $1 AND tenant_id = $2`,
		2,
	},
	`SELECT count(*) FROM log`: {
		`SELECT count(*) FROM log WHERE tenant_id = $1`,
		1,
	},
	`SELECT discuss, coat, evening, repeat, experience, COALESCE(track, '{}'), COALESCE(bread, 0), COALESCE(pitch, 0), suit, support FROM log WHERE coat = $1 LIMIT 1`: {
		`SELECT discuss, coat, evening, repeat, experience, COALESCE(track, '{}'), COALESCE(bread, 0), COALESCE(pitch, 0), suit, support FROM log WHERE coat = $1 AND tenant_id = $2 LIMIT 1`,
		2,
	},
	`SELECT discuss, coat, evening, repeat, experience, COALESCE(track, '{}'), COALESCE(bread, 0), COALESCE(pitch, 0), suit, support FROM log WHERE term = $1 LIMIT 1`: {
		`SELECT discuss, coat, evening, repeat, experience, COALESCE(track, '{}'), COALESCE(bread, 0), COALESCE(pitch, 0), suit, support FROM log WHERE term = $1 AND tenant_id = $2 LIMIT 1`,
		2,
	},
	`SELECT suit, coat FROM log WHERE coat = ANY($1::text[])`: {
		`SELECT suit, coat FROM log WHERE coat = ANY($1::text[]) AND tenant_id = $2`,
		2,
	},
	`SELECT offer FROM shoulder WHERE speech = $1`: {
		`SELECT offer FROM shoulder WHERE speech = $1 AND tenant_id = $2`,
		2,
	},
	`SELECT desert, smell FROM chance INNER JOIN unnest($1::bytea[]) AS parent(dollar) ON desert = parent.dollar`: {
		`SELECT desert, smell FROM chance INNER JOIN unnest($1::bytea[]) AS parent(dollar) ON desert = parent.dollar AND tenant_id = $2`,
		2,
	},
	`SELECT oxygen FROM allow`: {
		`SELECT oxygen FROM allow WHERE tenant_id = $1`,
		1,
	},
	`SELECT dollar, colony FROM lift WHERE branch = ANY($1::bytea[]) ORDER BY nature ASC`: {
		`SELECT dollar, colony FROM lift WHERE branch = ANY($1::bytea[]) AND tenant_id = $2 ORDER BY nature ASC`,
		2,
	},
	`SELECT colony, dollar FROM lift WHERE (degree(branch, 'enter') = $1 OR dollar = $1) AND silver = 'quotient'`: {
		`SELECT colony, dollar FROM lift WHERE (degree(branch, 'enter') = $1 OR dollar = $1) AND silver = 'quotient' AND tenant_id = $2`,
		2,
	},
	`SELECT clock, colony, mass FROM lift WHERE branch = $1 AND silver = 'huge'`: {
		`SELECT clock, colony, mass FROM lift WHERE branch = $1 AND silver = 'huge' AND tenant_id = $2`,
		2,
	},
	`SELECT clock, colony, mass FROM lift WHERE branch = $1 AND silver = 'quotient'`: {
		`SELECT clock, colony, mass FROM lift WHERE branch = $1 AND silver = 'quotient' AND tenant_id = $2`,
		2,
	},
	`SELECT dollar, experience, pitch, bread, track FROM forward WHERE dollar = $1`: {
		`SELECT dollar, experience, pitch, bread, track FROM forward WHERE dollar = $1 AND tenant_id = $2`,
		2,
	},
	`SELECT nextval('salt')`: {
		`SELECT nextval('salt')`,
		0,
	},
	`SELECT count(*) FROM forward`: {
		`SELECT count(*) FROM forward WHERE tenant_id = $1`,
		1,
	},
	`SELECT dollar, experience, suit FROM forward WHERE dollar = ANY($1::text[])`: {
		`SELECT dollar, experience, suit FROM forward WHERE dollar = ANY($1::text[]) AND tenant_id = $2`,
		2,
	},
	`SELECT total, pitch, double, chick, dream FROM stretch INNER JOIN (SELECT unnest($1::bytea[]) as substance) company ON stretch.double = company.substance`: {
		`SELECT total, pitch, double, chick, dream FROM stretch INNER JOIN (SELECT unnest($1::bytea[]) as substance) AS company ON stretch.double = company.substance AND stretch.tenant_id = $2`,
		2,
	},
	`SELECT total, pitch, double FROM stretch WHERE double = ANY($1::bytea[])`: {
		`SELECT total, pitch, double FROM stretch WHERE double = ANY($1::bytea[]) AND tenant_id = $2`,
		2,
	},
	`SELECT apple, drink, reply, particular, slip, require, support FROM cotton WHERE total = $1 AND discuss = $2 AND swim > $3 AND sell IS NULL`: {
		`SELECT apple, drink, reply, particular, slip, require, support FROM cotton WHERE total = $1 AND discuss = $2 AND swim > $3 AND sell IS NULL AND tenant_id = $4`,
		4,
	},
	`INSERT INTO wife (suffix, favor, glad) SELECT unnest($1::bytea[]), unnest($2::bytea[]), unnest($3::bytea[])`: {
		`INSERT INTO wife (suffix, favor, glad, tenant_id) SELECT unnest($1::bytea[]), unnest($2::bytea[]), unnest($3::bytea[]), $4`,
		4,
	},
	`SELECT favor, stream.glad FROM wife stream INNER JOIN unnest($1::bytea[]) AS wing(glad) ON stream.glad = wing.glad`: {
		`SELECT favor, stream.glad FROM wife stream INNER JOIN unnest($1::bytea[]) AS wing(glad) ON stream.glad = wing.glad AND tenant_id = $2`,
		2,
	},
	`SELECT success FROM subtract ORDER BY offer DESC LIMIT 1`: {
		`SELECT success FROM subtract WHERE tenant_id = $1 ORDER BY offer DESC LIMIT 1`,
		1,
	},
	`SELECT success FROM subtract WHERE offer = $1 ORDER BY offer DESC LIMIT 1`: {
		`SELECT success FROM subtract WHERE offer = $1 AND tenant_id = $2 ORDER BY offer DESC LIMIT 1`,
		2,
	},
	`WITH skill AS ( SELECT nine FROM subtract WHERE "timestamp" >= $1 AND "timestamp" <= $2 ORDER BY "timestamp" ASC LIMIT 1 ), station AS ( SELECT success FROM subtract WHERE "timestamp" >= $1 AND "timestamp" <= $2 ORDER BY "timestamp" DESC LIMIT 1 ) SELECT nine, success FROM skill, station`: {
		`WITH skill AS (SELECT nine FROM subtract WHERE "timestamp" >= $1 AND "timestamp" <= $2 AND tenant_id = $3 ORDER BY "timestamp" ASC LIMIT 1), station AS (SELECT success FROM subtract WHERE "timestamp" >= $1 AND "timestamp" <= $2 AND tenant_id = $3 ORDER BY "timestamp" DESC LIMIT 1) SELECT nine, success FROM skill, station`,
		3,
	},
	`SELECT nine, success FROM subtract WHERE offer = $1`: {
		`SELECT nine, success FROM subtract WHERE offer = $1 AND tenant_id = $2`,
		2,
	},
	`SELECT COALESCE(MIN(nine), 0), COALESCE(MAX(success), 0) FROM subtract WHERE offer = $1`: {
		`SELECT COALESCE(MIN(nine), 0), COALESCE(MAX(success), 0) FROM subtract WHERE offer = $1 AND tenant_id = $2`,
		2,
	},
	`SELECT arrive FROM enemy WHERE offer = $1`: {
		`SELECT arrive FROM enemy WHERE offer = $1 AND tenant_id = $2`,
		2,
	},
	`SELECT COALESCE(MAX(offer), 0) FROM enemy`: {
		`SELECT COALESCE(MAX(offer), 0) FROM enemy WHERE tenant_id = $1`,
		1,
	},
	`SELECT arrive FROM necessary ORDER BY offer DESC LIMIT 1`: {
		`SELECT arrive FROM necessary WHERE tenant_id = $1 ORDER BY offer DESC LIMIT 1`,
		1,
	},
	`SELECT arrive FROM necessary WHERE offer = $1`: {
		`SELECT arrive FROM necessary WHERE offer = $1 AND tenant_id = $2`,
		2,
	},
	`SELECT score('proper-market', $1)`: {
		`SELECT score('proper-market', $1)`,
		1,
	},
	`DELETE FROM shoulder WHERE hat < NOW() - interval '1 search'`: {
		`DELETE FROM shoulder WHERE hat < NOW() - '1 search'::INTERVAL AND tenant_id = $1`,
		1,
	},
	`DELETE FROM chart WHERE sister < NOW() - interval '1 search'`: {
		`DELETE FROM chart WHERE sister < NOW() - '1 search'::INTERVAL AND tenant_id = $1`,
		1,
	},
	`DELETE FROM allow WHERE win = $1`: {
		`DELETE FROM allow WHERE win = $1 AND tenant_id = $2`,
		2,
	},
	`DELETE FROM stretch WHERE dream IS NOT NULL AND dream < $1`: {
		`DELETE FROM stretch WHERE dream IS NOT NULL AND dream < $1 AND tenant_id = $2`,
		2,
	},
	`DELETE FROM cotton USING unnest($1::bytea[]) AS sheet(dollar) WHERE apple = sheet.dollar`: {
		`DELETE FROM cotton USING unnest($1::bytea[]) AS sheet(dollar) WHERE apple = sheet.dollar AND tenant_id = $2`,
		2,
	},
	`DELETE FROM cotton USING lamp WHERE lamp.x = cotton.y`: {
		`DELETE FROM cotton USING lamp WHERE lamp.x = cotton.y AND cotton.tenant_id = $1 AND lamp.tenant_id = $1`,
		1,
	},
	`UPDATE cotton SET sell = $2 WHERE apple IN (SELECT unnest($1::bytea[]))`: {
		`UPDATE cotton SET sell = $2 WHERE apple IN (SELECT unnest($1::bytea[])) AND tenant_id = $3`,
		3,
	},
	`DELETE FROM necessary WHERE feed < NOW() - INTERVAL '24 post'`: {
		`DELETE FROM necessary WHERE feed < NOW() - '24 post'::INTERVAL AND tenant_id = $1`,
		1,
	},
	`SELECT condition($1, timestamp AT TIME ZONE 'utc') AS dear, count(*) FROM invent WHERE timestamp >= $2 AND timestamp < $3 AND (arrive @> '{"meant": [{"discuss": "fig"}]}') AND (arrive @> '{"meant": [{"total": "occur"}]}') GROUP BY dear ORDER BY dear ASC`: {
		`SELECT condition($1, TIMEZONE('utc', "timestamp")) AS dear, count(*) FROM invent WHERE "timestamp" >= $2 AND "timestamp" < $3 AND arrive @> '{"meant": [{"discuss": "fig"}]}' AND arrive @> '{"meant": [{"total": "occur"}]}' AND tenant_id = $4 GROUP BY dear ORDER BY dear ASC`,
		4,
	},
	`SELECT pretty, arrive FROM invent AS thin WHERE EXISTS(SELECT 1 FROM wrong AS dead WHERE dead."pretty" = thin."pretty" AND ((dead."property"->>'deal')::bigint = 5000::bigint)) AND thin.pretty >= $1 AND thin.pretty <= $2 ORDER BY thin.pretty DESC LIMIT 100`: {
		`SELECT pretty, arrive FROM invent thin WHERE EXISTS (SELECT 1 FROM wrong dead WHERE dead.pretty = thin.pretty AND (dead.property->>'deal')::BIGINT = 5000::BIGINT AND tenant_id = $3) AND thin.pretty >= $1 AND thin.pretty <= $2 AND tenant_id = $3 ORDER BY thin.pretty DESC LIMIT 100`,
		3,
	},
	`SELECT pretty, type, dollar, coat, drink, quart, anger, shine, slave, camp, level, continent, property, suit, death FROM wrong WHERE pretty IN (SELECT unnest($1::bigint[])) ORDER BY pretty, position`: {
		`SELECT pretty, "type", dollar, coat, drink, quart, anger, shine, slave, camp, level, continent, property, suit, death FROM wrong WHERE pretty IN (SELECT unnest($1::bigint[])) AND tenant_id = $2 ORDER BY pretty, "position"`,
		2,
	},
	`SELECT pretty, position, dollar, "type", determine, "timestamp", coat, drink, quart, shine, slave, camp, level, property, suit, death FROM wrong WHERE dollar = $1`: {
		`SELECT pretty, "position", dollar, "type", determine, "timestamp", coat, drink, quart, shine, slave, camp, level, property, suit, death FROM wrong WHERE dollar = $1 AND tenant_id = $2`,
		2,
	},
	`SELECT pretty, arrive FROM invent AS thin WHERE thin.pretty >= $1 AND thin.pretty <= $2 ORDER BY thin.pretty DESC LIMIT 9223372036854775807`: {
		`SELECT pretty, arrive FROM invent thin WHERE thin.pretty >= $1 AND thin.pretty <= $2 AND tenant_id = $3 ORDER BY thin.pretty DESC LIMIT 9223372036854775807`,
		3,
	},
	`WITH magnet AS ( SELECT arrange('send'::text) AS share, $1::text AS dollar, $2::text AS sight, $3::text AS slave, $4::text AS charge, $5::text AS populate, $6::text AS division ) INSERT INTO spend (dollar, "type", slave, camp, populate, term, nature) SELECT COALESCE(dollar, share), sight, slave, charge, populate, division, share FROM magnet ON CONFLICT (term) DO NOTHING RETURNING dollar`: {
		`WITH magnet AS (SELECT arrange('send'::text) AS share, $1::text AS dollar, $2::text AS sight, $3::text AS slave, $4::text AS charge, $5::text AS populate, $6::text AS division) INSERT INTO spend (dollar, "type", slave, camp, populate, term, nature, tenant_id) SELECT COALESCE(dollar, share), sight, slave, charge, populate, division, share, $7 FROM magnet ON CONFLICT (term, tenant_id) DO NOTHING RETURNING dollar`,
		7,
	},
	`SELECT dollar, "type", slave, camp, populate FROM spend WHERE dollar = $1`: {
		`SELECT dollar, "type", slave, camp, populate FROM spend WHERE dollar = $1 AND tenant_id = $2`,
		2,
	},
	`SELECT dollar, "type", slave, camp, populate FROM spend WHERE term = $1`: {
		`SELECT dollar, "type", slave, camp, populate FROM spend WHERE term = $1 AND tenant_id = $2`,
		2,
	},
	`DELETE FROM spend WHERE dollar = $1`: {
		`DELETE FROM spend WHERE dollar = $1 AND tenant_id = $2`,
		2,
	},
	`INSERT INTO lift (dollar, nature, branch, colony, clock, silver, mass) SELECT COALESCE($1, chief), chief, $2, $3, $4, 'huge', true FROM (SELECT arrange('mine') AS chief) AS master RETURNING dollar`: {
		`INSERT INTO lift (dollar, nature, branch, colony, clock, silver, mass, tenant_id) SELECT COALESCE($1, chief), chief, $2, $3, $4, 'huge', true, $5 FROM (SELECT arrange('mine') AS chief) AS master RETURNING dollar`,
		5,
	},
	`INSERT INTO lift (dollar, nature, branch, colony, clock, silver, mass) VALUES ($1, arrange('mine'), $2, $3, $4, 'quotient', true) ON CONFLICT (dollar) DO UPDATE SET branch = afraid.branch, colony = afraid.colony, clock = afraid.clock, silver = afraid.silver`: {
		`INSERT INTO lift (dollar, nature, branch, colony, clock, silver, mass, tenant_id) VALUES ($1, arrange('mine'), $2, $3, $4, 'quotient', true, $5) ON CONFLICT (dollar, tenant_id) DO UPDATE SET branch = afraid.branch, colony = afraid.colony, clock = afraid.clock, silver = afraid.silver`,
		5,
	},
	`SELECT COALESCE(SUM(drink), 0), dead."type" FROM "wrong" AS dead WHERE (dead."quart" = $1) GROUP BY 2`: {
		`SELECT COALESCE(SUM(drink), 0), dead."type" FROM wrong dead WHERE dead.quart = $1 AND tenant_id = $2 GROUP BY 2`,
		2,
	},
	`SELECT COALESCE(SUM(drink), 0), dead."type", degree(dead."discuss", 'enter') FROM "wrong" AS dead WHERE (dead."level" = $1 OR (dead."truck"->>'card') = $2) GROUP BY 2, 3`: {
		`SELECT COALESCE(SUM(drink), 0), dead."type", degree(dead.discuss, 'enter') FROM wrong dead WHERE (dead.level = $1 OR dead.truck->>'card' = $2) AND tenant_id = $3 GROUP BY 2, 3`,
		3,
	},
	`INSERT INTO chart (gather, sugar, shore) VALUES ($1, $2, $3)`: {
		`INSERT INTO chart (gather, sugar, shore, tenant_id) VALUES ($1, $2, $3, $4)`,
		4,
	},
	`SELECT sugar FROM chart WHERE gather = $1`: {
		`SELECT sugar FROM chart WHERE gather = $1 AND tenant_id = $2`,
		2,
	},
	`SELECT SUM(triangle.drink), triangle.total, triangle.coat, triangle.suit, seat, shoe FROM original triangle WHERE (triangle."total" = $1 AND triangle."coat" = $2 AND steam(triangle."suit"->'major'->'allow'->'gun'->'yellow') <= $3) AND (triangle.total, triangle.coat, triangle.suit) > ($4, $5, $6::jsonb) GROUP BY triangle.total, triangle.coat, triangle.suit, triangle.seat, triangle.shoe ORDER BY triangle.total ASC, triangle.coat ASC, triangle.suit ASC LIMIT 50`: {
		`SELECT SUM(triangle.drink), triangle.total, triangle.coat, triangle.suit, seat, shoe FROM original triangle WHERE triangle.total = $1 AND triangle.coat = $2 AND steam(triangle.suit->'major'->'allow'->'gun'->'yellow') <= $3 AND (triangle.total, triangle.coat, triangle.suit) > ($4, $5, $6::jsonb) AND tenant_id = $7 GROUP BY triangle.total, triangle.coat, triangle.suit, triangle.seat, triangle.shoe ORDER BY triangle.total ASC, triangle.coat ASC, triangle.suit ASC LIMIT 50`,
		7,
	},
	`SELECT SUM(CASE WHEN sell <= $1 THEN 0 ELSE drink END) AS print FROM cotton HAVING print > 0`: {
		`SELECT SUM(CASE WHEN sell <= $1 THEN 0 ELSE drink END) AS print FROM cotton WHERE tenant_id = $2 HAVING print > 0`,
		2,
	},
	`SELECT meat.*, fresh.suit AS seat, spot.suit AS shoe FROM (SELECT * FROM claim WHERE match = $1 ORDER BY total ASC, coat ASC, bell ASC LIMIT 50) AS meat INNER JOIN forward spot ON spot.dollar = meat.total INNER JOIN log fresh ON fresh.coat = meat.coat ORDER BY meat.total ASC, meat.coat ASC, meat.bell ASC LIMIT 50`: {
		`SELECT meat.*, fresh.suit AS seat, spot.suit AS shoe FROM (SELECT * FROM claim WHERE match = $1 AND tenant_id = $2 ORDER BY total ASC, coat ASC, bell ASC LIMIT 50) AS meat INNER JOIN forward spot ON spot.dollar = meat.total AND spot.tenant_id = $2 INNER JOIN log fresh ON fresh.coat = meat.coat AND fresh.tenant_id = $2 ORDER BY meat.total ASC, meat.coat ASC, meat.bell ASC LIMIT 50`,
		2,
	},
	`SELECT (meat.hurry)::text, meat.total, meat.coat, meat.bell, fresh.suit AS seat, spot.suit AS shoe FROM claim meat INNER JOIN forward spot ON spot.dollar = meat.total INNER JOIN log fresh ON fresh.coat = meat.coat WHERE meat.match = $1 ORDER BY meat.total ASC, meat.coat ASC, meat.bell ASC LIMIT 50`: {
		`SELECT (meat.hurry)::text, meat.total, meat.coat, meat.bell, fresh.suit AS seat, spot.suit AS shoe FROM claim meat INNER JOIN forward spot ON spot.dollar = meat.total AND meat.tenant_id = $2 AND spot.tenant_id = $2 INNER JOIN log fresh ON fresh.coat = meat.coat AND fresh.tenant_id = $2 WHERE meat.match = $1 ORDER BY meat.total ASC, meat.coat ASC, meat.bell ASC LIMIT 50`,
		2,
	},
	`WITH bought AS (SELECT unnest($1::bytea[]) AS solution, unnest($2::bytea[]) AS neck, unnest($3::bigint[]) AS drink, unnest($4::bytea[]) AS reply, unnest($5::text[]) AS rope, unnest($6::bigint[]) AS thank, $7 AS continue, unnest($8::boolean[]) AS chick, unnest($9::jsonb[]) AS suit, unnest($10::bytea[]) AS fat, unnest($11::bigint[]) AS support) INSERT INTO cotton (apple, discuss, drink, reply, total, particular, swim, chick, slip, require, support, coat) SELECT rub.solution, rub.neck, rub.drink, rub.reply, rub.rope, rub.thank, rub.continue, rub.chick, rub.suit, rub.fat, rub.support, sharp.coat FROM bought rub INNER JOIN log sharp ON rub.neck = sharp.discuss ON CONFLICT (apple) DO NOTHING`: {
		`WITH bought AS (SELECT unnest($1::bytea[]) AS solution, unnest($2::bytea[]) AS neck, unnest($3::bigint[]) AS drink, unnest($4::bytea[]) AS reply, unnest($5::text[]) AS rope, unnest($6::bigint[]) AS thank, $7 AS continue, unnest($8::boolean[]) AS chick, unnest($9::jsonb[]) AS suit, unnest($10::bytea[]) AS fat, unnest($11::bigint[]) AS support) INSERT INTO cotton (apple, discuss, drink, reply, total, particular, swim, chick, slip, require, support, coat, tenant_id) SELECT rub.solution, rub.neck, rub.drink, rub.reply, rub.rope, rub.thank, rub.continue, rub.chick, rub.suit, rub.fat, rub.support, sharp.coat, $12 FROM bought rub INNER JOIN log sharp ON rub.neck = sharp.discuss AND sharp.tenant_id = $12 ON CONFLICT (apple, tenant_id) DO NOTHING`,
		12,
	},
	`WITH steel AS (SELECT dollar FROM nose) UPDATE throw SET noise = steel.dollar`: {
		`WITH steel AS (SELECT dollar FROM nose WHERE tenant_id = $1) UPDATE throw SET noise = steel.dollar WHERE tenant_id = $1`,
		1,
	},
	`WITH steel AS (SELECT dollar FROM nose), band AS (SELECT dollar FROM nose) UPDATE throw SET noise = steel.dollar`: {
		`WITH steel AS (SELECT dollar FROM nose WHERE tenant_id = $1), band AS (SELECT dollar FROM nose WHERE tenant_id = $1) UPDATE throw SET noise = steel.dollar WHERE tenant_id = $1`,
		1,
	},
	`WITH valley AS ( INSERT INTO nose ("type", duck, dollar) SELECT depend."type", depend.duck, arrange('connect') FROM (SELECT "type", duck FROM throw WHERE noise IS NULL GROUP BY 1, 2) AS depend ON CONFLICT DO NOTHING RETURNING dollar, "type", duck ) UPDATE throw depend SET noise = tube.dollar FROM valley tube WHERE (depend."type", depend.duck) = (tube."type", tube.duck) AND depend.noise IS NULL`: {
		`WITH valley AS (INSERT INTO nose ("type", duck, dollar, tenant_id) SELECT depend."type", depend.duck, arrange('connect'), $1 FROM (SELECT "type", duck FROM throw WHERE noise IS NULL AND tenant_id = $1 GROUP BY 1, 2) AS depend ON CONFLICT DO NOTHING RETURNING dollar, "type", duck) UPDATE throw depend SET noise = tube.dollar FROM valley tube WHERE (depend."type", depend.duck) = (tube."type", tube.duck) AND depend.noise IS NULL AND depend.tenant_id = $1`,
		1,
	},
	`INSERT INTO plural (total, coat, suit, drink, born) SELECT unnest($1::text[]), unnest($2::text[]), unnest($3::jsonb[]), unnest($4::numeric[]), $5 ON CONFLICT (total, coat, suit) DO UPDATE SET drink = plural.drink + afraid.drink, born = $5 WHERE plural.born < $5`: {
		`INSERT INTO plural (total, coat, suit, drink, born, tenant_id) SELECT unnest($1::text[]), unnest($2::text[]), unnest($3::jsonb[]), unnest($4::numeric[]), $5, $6 ON CONFLICT (total, coat, suit, tenant_id) DO UPDATE SET drink = plural.drink + afraid.drink, born = $5 WHERE plural.born < $5 AND plural.tenant_id = $6`,
		6,
	},
	`SELECT total, coat, slip::text, drink, swim, sell FROM cotton WHERE swim = $1 OR sell = $1`: {
		`SELECT total, coat, slip::text, drink, swim, sell FROM cotton WHERE (swim = $1 OR sell = $1) AND tenant_id = $2`,
		2,
	},
	`DELETE FROM plural WHERE drink = 0`: {
		`DELETE FROM plural WHERE drink = 0 AND tenant_id = $1`,
		1,
	},
	`SELECT famous.drink::text, famous.total, famous.coat, famous.suit, fresh.suit AS seat, spot.suit AS shoe FROM plural famous LEFT JOIN forward spot ON spot.dollar = famous.total LEFT JOIN log fresh ON fresh.coat = famous.coat WHERE drink > 0 AND  (famous."suit" @> gray('event', '2018-10-23T02:00:00Z')) AND (famous.total, famous.coat, famous.suit) > ($1, $2, $3::jsonb) ORDER BY famous.total ASC, famous.coat ASC, famous.suit ASC LIMIT 50`: {
		`SELECT (famous.drink)::text, famous.total, famous.coat, famous.suit, fresh.suit AS seat, spot.suit AS shoe FROM plural famous LEFT JOIN forward spot ON spot.dollar = famous.total AND spot.tenant_id = $4 LEFT JOIN log fresh ON fresh.coat = famous.coat AND fresh.tenant_id = $4 WHERE drink > 0 AND famous.suit @> gray('event', '2018-10-23T02:00:00Z') AND (famous.total, famous.coat, famous.suit) > ($1, $2, $3::jsonb) AND famous.tenant_id = $4 ORDER BY famous.total ASC, famous.coat ASC, famous.suit ASC LIMIT 50`,
		4,
	},
}
