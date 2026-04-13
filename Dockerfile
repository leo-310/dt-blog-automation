FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml README.md /app/
COPY src /app/src
COPY data /app/data
COPY prompts /app/prompts
COPY content /app/content

RUN pip install --no-cache-dir -e .

ENV BLOG_AGENT_API_HOST=0.0.0.0
ENV BLOG_AGENT_API_PORT=10000

EXPOSE 10000

CMD ["blog-agent-api"]
