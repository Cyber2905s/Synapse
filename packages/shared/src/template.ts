const VAR = /\{\{\s*([\w.]+)\s*\}\}/g;

/**
 * Renders `{{var}}` / `{{nested.path}}` placeholders. Missing values render as an empty string;
 * event payloads are zod-validated at ingestion, so every route's variables exist in practice.
 */
export function render(template: string, vars: Record<string, unknown>): string {
  return template.replace(VAR, (_, path: string) => {
    let value: unknown = vars;
    for (const key of path.split('.')) {
      value =
        value !== null && typeof value === 'object'
          ? (value as Record<string, unknown>)[key]
          : undefined;
    }
    return value === undefined || value === null ? '' : String(value);
  });
}
