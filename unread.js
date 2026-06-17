// Single source of truth for "unread messages for `agent`", including topic routing.
// Takes a db handle (each process opens its own DatabaseSync) so server + watchers agree.
//
// Topic routing: agents with a topics subscription only see topic-tagged broadcasts
// they subscribed to. topics=NULL (never joined with topics) = see all (backward-compat).
// Directed messages and non-topic broadcasts always pass through routing.
//
// Optional filters:
//   opts.from  — only messages from this agent
//   opts.topic — only messages tagged with this topic
export function unreadFor(db, agent, { from, topic } = {}) {
  let sql = `SELECT m.* FROM messages m
    WHERE m.from_agent != ?
      AND (m.to_agent = ? OR m.to_agent IS NULL)
      AND m.kind != 'response'
      AND NOT EXISTS (SELECT 1 FROM reads r WHERE r.agent = ? AND r.message_id = m.id)`;
  const params = [agent, agent, agent];

  if (from) { sql += ` AND m.from_agent = ?`; params.push(from); }
  if (topic) { sql += ` AND m.topic = ?`; params.push(topic); }
  sql += ` ORDER BY m.id`;

  let msgs = db.prepare(sql).all(...params);

  // Topic routing (JS layer): agents with a topics subscription only see topic-tagged
  // broadcasts they subscribed to. topics=null = no subscription set = see all (backwards compat).
  const agentRow = db.prepare("SELECT topics FROM agents WHERE name = ?").get(agent);
  const myTopics = agentRow?.topics ? JSON.parse(agentRow.topics) : null;

  if (myTopics !== null) {
    msgs = msgs.filter((m) => {
      if (m.to_agent || !m.topic) return true; // directed msg or non-topic broadcast: always show
      return myTopics.includes(m.topic);        // topic broadcast: check subscription
    });
  }

  return msgs;
}
