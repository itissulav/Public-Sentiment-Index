-- Phase 2 Optimized Relational Schema
-- Run this in your Supabase SQL Editor to upgrade your database!

-- 1. Create the topics table
CREATE TABLE IF NOT EXISTS search_topics (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name TEXT UNIQUE NOT NULL,
    total_comments INTEGER DEFAULT 0,
    last_updated TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc', now())
);

-- 2. Create the comments table with a foreign key to topics
CREATE TABLE IF NOT EXISTS reddit_comments (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    topic_id UUID REFERENCES search_topics(id) ON DELETE CASCADE,
    post_id TEXT NOT NULL,
    text TEXT NOT NULL,
    author TEXT NOT NULL,
    score INTEGER NOT NULL,
    sentiment_label TEXT NOT NULL,
    confidence_score FLOAT NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE,
    analyzed_date TIMESTAMP WITH TIME ZONE DEFAULT timezone('utc', now())
);

-- 3. Create an Index for blazing fast querying by topic
CREATE INDEX IF NOT EXISTS idx_reddit_comments_topic_id ON reddit_comments(topic_id);
