-- Table: ytvideo_mdextractor_channels
CREATE TABLE ytvideo_mdextractor_channels (
    channel_id VARCHAR(255) PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    description TEXT,
    subscriber_count BIGINT,
    total_views BIGINT,
    total_videos INT,
    creation_date DATE,
    country VARCHAR(255),
    language VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Table: ytvideo_mdextractor_videos
CREATE TABLE ytvideo_mdextractor_videos (
    video_id VARCHAR(255) PRIMARY KEY,
    channel_id VARCHAR(255) NOT NULL,
    title VARCHAR(255) NOT NULL,
    description TEXT,
    thumbnail_url VARCHAR(255),
    publish_date TIMESTAMP,
    view_count BIGINT,
    like_count INT,
    dislike_count INT,
    comment_count INT,
    duration INT, -- Stored in seconds
    tags TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (channel_id) REFERENCES ytvideo_mdextractor_channels(channel_id) ON DELETE CASCADE
);

-- Table: ytvideo_mdextractor_playlists
CREATE TABLE ytvideo_mdextractor_playlists (
    playlist_id VARCHAR(255) PRIMARY KEY,
    channel_id VARCHAR(255) NOT NULL,
    title VARCHAR(255) NOT NULL,
    description TEXT,
    creation_date DATE,
    total_videos INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (channel_id) REFERENCES ytvideo_mdextractor_channels(channel_id) ON DELETE CASCADE
);

-- Table: ytvideo_mdextractor_playlist_videos
CREATE TABLE ytvideo_mdextractor_playlist_videos (
    playlist_id VARCHAR(255) NOT NULL,
    video_id VARCHAR(255) NOT NULL,
    video_order INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (playlist_id, video_id, video_order),
    FOREIGN KEY (playlist_id) REFERENCES ytvideo_mdextractor_playlists(playlist_id) ON DELETE CASCADE,
    FOREIGN KEY (video_id) REFERENCES ytvideo_mdextractor_videos(video_id) ON DELETE CASCADE
);

-- Unique constraint for playlist videos to prevent duplicate video_order
ALTER TABLE ytvideo_mdextractor_playlist_videos
    ADD CONSTRAINT unique_playlist_video_order UNIQUE (playlist_id, video_id, video_order);

-- Optional: Indexes to optimize queries
CREATE INDEX idx_channel_title ON ytvideo_mdextractor_channels(title);
CREATE INDEX idx_video_title ON ytvideo_mdextractor_videos(title);
CREATE INDEX idx_playlist_title ON ytvideo_mdextractor_playlists(title);

-- Trigger function to update the `updated_at` column
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create triggers to automatically update `updated_at` on row update
CREATE TRIGGER update_channel_updated_at
BEFORE UPDATE ON ytvideo_mdextractor_channels
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_video_updated_at
BEFORE UPDATE ON ytvideo_mdextractor_videos
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_playlist_updated_at
BEFORE UPDATE ON ytvideo_mdextractor_playlists
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_playlist_video_updated_at
BEFORE UPDATE ON ytvideo_mdextractor_playlist_videos
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();