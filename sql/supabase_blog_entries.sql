create extension if not exists pgcrypto;

create table if not exists public.blog_entries (
  id uuid primary key default gen_random_uuid(),
  pipeline_id text not null unique,
  post_id text,
  title text not null,
  query text not null,
  cluster text not null,
  pillar_id text,
  pillar_name text,
  pillar_claim text,
  main_topic text,
  sub_blog_tag text,
  is_pillar_head boolean not null default false,
  pillar_head_post_id text,
  pillar_head_slug text,
  planned_keywords jsonb not null default '[]'::jsonb,
  path text,
  scheduled_for date not null,
  status text not null,
  topic_role text not null,
  created_at timestamptz,
  approved_at timestamptz,
  pushed_at timestamptz,
  shopify_article_id text,
  shopify_blog_id text,
  shopify_article_handle text,
  topic_angle text,
  topic_outline jsonb not null default '[]'::jsonb,
  topic_internal_links jsonb not null default '[]'::jsonb,
  guideline_report jsonb,
  pipeline_metadata jsonb not null default '{}'::jsonb,
  post_frontmatter jsonb,
  post_markdown text,
  history_title text,
  history_slug text,
  history_query text,
  history_cluster text,
  history_created_on date,
  generated_image_file text,
  generated_image_mime_type text,
  generated_image_base64 text,
  synced_at timestamptz not null default now()
);

create index if not exists blog_entries_status_idx on public.blog_entries(status);
create index if not exists blog_entries_scheduled_for_idx on public.blog_entries(scheduled_for desc);
create index if not exists blog_entries_updated_idx on public.blog_entries(synced_at desc);

alter table public.blog_entries enable row level security;
