const originalFetch = globalThis.fetch;

globalThis.fetch = async (input, init) => {
  const url = new URL(String(input));
  if (url.hostname !== 'ingest.example.test') {
    return originalFetch(input, init);
  }
  if (url.pathname === '/health') return new Response('{"status":"ok"}');
  if (url.pathname === '/v1/auth/verify') {
    return new Response('{"status":"ok"}');
  }
  return new Response('{"error":"NOT_FOUND"}', { status: 404 });
};
