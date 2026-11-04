/**
 * Failures the adapter raises before a prompt reaches the cell. Each carries
 * a message written for the thread's error row, because the orchestrator
 * shows the nested cause of a session-open or thread-load failure as is.
 */
import { RuntimeMode } from "@t3tools/contracts";
import * as Schema from "effect/Schema";

export const CELL_RUNTIME_MODE_MESSAGE =
  "Cell runs only in Full access. Set this thread's permission mode to Full access and send the message again.";

export class CellRuntimeModeError extends Schema.TaggedError<CellRuntimeModeError>()(
  "CellRuntimeModeError",
  { runtimeMode: RuntimeMode },
) {
  override get message(): string {
    return CELL_RUNTIME_MODE_MESSAGE;
  }
}

export class CellModelError extends Schema.TaggedError<CellModelError>()("CellModelError", {
  requested: Schema.String,
  available: Schema.String,
}) {
  override get message(): string {
    return `This cell runs ${this.available} and cannot switch to ${this.requested}. Pick Default or ${this.available} in the model picker and send the message again.`;
  }
}

/**
 * How a tenant refuses T3 rather than one request: 401 no session (no token, or an ended one), 403
 * judged another site's or not allowed, 503 a base domain without SESSION_SECRET. 4401 is a socket's 401.
 */
export const CELL_ACCESS_STATUSES: ReadonlySet<number> = new Set([401, 403, 503]);
/** The close code a cell gives a socket whose session no longer holds. */
export const CELL_SESSION_CLOSE = 4401;

function tenantName(baseUrl: string): string {
  try {
    return new URL(baseUrl).host;
  } catch {
    return baseUrl;
  }
}

const TOKEN_STEPS = (baseUrl: string) =>
  `Log in at ${baseUrl.replace(/\/+$/, "")}/login, choose Create an API token, and paste the token into Settings → Providers → Cell → Access token.`;

/**
 * The cell's own words: the body's first line, bounded, without the 401's note on how to log in
 * (the message gives T3's steps, and redaction would mangle "Authorization: Bearer"). HTML or empty: null.
 */
function cellRefusalText(body: string): string | null {
  const line = (body.trim().split("\n", 1)[0] ?? "")
    .replace(/;\s*log in at \/login\b.*$/, "")
    .trim();
  if (line === "" || line.startsWith("<")) return null;
  return line.length > 400 ? `${line.slice(0, 399)}…` : line;
}

/**
 * The tenant refused T3's credentials or serves nothing: what the cell said
 * and what the user does about it. One message for the status probe, a
 * thread that cannot attach, a prompt refused and a socket the cell closed.
 */
export class CellAccessError extends Schema.TaggedError<CellAccessError>()("CellAccessError", {
  baseUrl: Schema.String,
  status: Schema.Number,
  /** Whether the instance has a token to send; a 401 without one says so. */
  tokenSet: Schema.Boolean,
  /** The cell's text, see `cellRefusalText`. */
  said: Schema.NullOr(Schema.String),
}) {
  /** The failure code T3 records with the turn. */
  get code(): string {
    return this.status === 503 ? "cell_unavailable" : "cell_unauthorized";
  }

  override get message(): string {
    const said = this.said === null ? ` (HTTP ${this.status})` : ` The tenant said: ${this.said}`;
    // Named by its host: T3's failure redaction rewrites a bare origin with a trailing slash.
    const tenant = tenantName(this.baseUrl);
    if (this.status === 503)
      return `The cell tenant ${tenant} is not serving requests; whoever runs it has to finish its setup.${said}`;
    if (this.status === 403) return `The cell tenant ${tenant} refused T3's request.${said}`;
    if (!this.tokenSet)
      return `The cell tenant ${tenant} needs an access token. ${TOKEN_STEPS(this.baseUrl)}${said}`;
    return `The cell tenant ${tenant} refused T3's access token. A token ends after 90 days, when the tenant's passphrase is reset with a new invite code, and when the operator suspends the tenant. ${TOKEN_STEPS(this.baseUrl)}${said}`;
  }
}

export const cellAccessError = (input: {
  readonly baseUrl: string;
  readonly token: string;
  readonly status: number;
  readonly body: string;
}) =>
  new CellAccessError({
    baseUrl: input.baseUrl.replace(/\/+$/, ""),
    status: input.status === CELL_SESSION_CLOSE ? 401 : input.status,
    tokenSet: input.token !== "",
    said: cellRefusalText(input.body),
  });
