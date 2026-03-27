FROM python:3.12-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates gnupg \
    && mkdir -p /etc/apt/keyrings \
    && curl -fsSL https://deb.nodesource.com/gpgkey/nodesource-repo.gpg.key \
      | gpg --dearmor -o /etc/apt/keyrings/nodesource.gpg \
    && echo "deb [signed-by=/etc/apt/keyrings/nodesource.gpg] https://deb.nodesource.com/node_22.x nodistro main" \
      > /etc/apt/sources.list.d/nodesource.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md /app/
COPY src /app/src
COPY data /app/data
COPY prompts /app/prompts
COPY content /app/content
COPY package.json package-lock.json index.html vite.config.js /app/

RUN pip install --no-cache-dir -e . \
    && npm ci \
    && npm run build

ENV BLOG_AGENT_API_HOST=0.0.0.0
ENV BLOG_AGENT_API_PORT=10000

EXPOSE 10000

CMD ["blog-agent-api"]
