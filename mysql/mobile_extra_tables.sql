CREATE TABLE IF NOT EXISTS user_favorite_group (
    object_id VARCHAR(36) PRIMARY KEY,
    user_id VARCHAR(36) NOT NULL,
    group_name VARCHAR(100) NOT NULL,
    create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_user_group (user_id, group_name)
);

CREATE TABLE IF NOT EXISTS user_privacy_setting (
    object_id VARCHAR(36) PRIMARY KEY,
    user_id VARCHAR(36) NOT NULL,
    favorites_visible TINYINT NOT NULL DEFAULT 1,
    likes_visible TINYINT NOT NULL DEFAULT 1,
    comments_visible TINYINT NOT NULL DEFAULT 1,
    uploads_visible TINYINT NOT NULL DEFAULT 1,
    create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_user_privacy (user_id)
);

-- =========================================================
-- Mobile API required extra table: user_favorite
-- Used by favorite group summary, add favorite, update favorite group, delete favorite
-- =========================================================
CREATE TABLE IF NOT EXISTS user_favorite (
    object_id VARCHAR(36) PRIMARY KEY,
    user_id VARCHAR(36) NOT NULL,
    artifact_id VARCHAR(36) NOT NULL,
    group_name VARCHAR(100) NOT NULL DEFAULT 'default',
    create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_user_artifact (user_id, artifact_id),
    KEY idx_user_favorite_user_id (user_id),
    KEY idx_user_favorite_artifact_id (artifact_id),
    KEY idx_user_favorite_group (user_id, group_name)
);

-- =========================================================
-- Mobile API required columns for artifact search and favorite response
-- image_url and image_path may be missing in some local initialized databases
-- The following statements are safe to run repeatedly.
-- =========================================================
SET @sql_image_url = (
    SELECT IF(
        COUNT(*) = 0,
        'ALTER TABLE artifact ADD COLUMN image_url VARCHAR(1000) NULL',
        'SELECT 1'
    )
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name = 'artifact'
      AND column_name = 'image_url'
);

PREPARE stmt_image_url FROM @sql_image_url;
EXECUTE stmt_image_url;
DEALLOCATE PREPARE stmt_image_url;

SET @sql_image_path = (
    SELECT IF(
        COUNT(*) = 0,
        'ALTER TABLE artifact ADD COLUMN image_path VARCHAR(500) NULL',
        'SELECT 1'
    )
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name = 'artifact'
      AND column_name = 'image_path'
);

PREPARE stmt_image_path FROM @sql_image_path;
EXECUTE stmt_image_path;
DEALLOCATE PREPARE stmt_image_path;

-- =========================================================
-- Mobile profile API required column: user.bio
-- Used by current-user and edit-profile APIs.
-- =========================================================
SET @sql_user_bio = (
    SELECT IF(
        COUNT(*) = 0,
        'ALTER TABLE user ADD COLUMN bio VARCHAR(500) NULL',
        'SELECT 1'
    )
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name = 'user'
      AND column_name = 'bio'
);

PREPARE stmt_user_bio FROM @sql_user_bio;
EXECUTE stmt_user_bio;
DEALLOCATE PREPARE stmt_user_bio;

-- =========================================================
-- Mobile user upload photos
-- Used by artifact detail page upload and my uploads page.
-- =========================================================
CREATE TABLE IF NOT EXISTS user_artifact_upload (
    object_id VARCHAR(36) PRIMARY KEY,
    user_id VARCHAR(36) NOT NULL,
    artifact_id VARCHAR(36) NOT NULL,
    image_path VARCHAR(1000) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    review_time DATETIME NULL,
    review_comment VARCHAR(500) NULL,
    create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    KEY idx_user_upload_user_time (user_id, create_time),
    KEY idx_user_upload_artifact (artifact_id),
    KEY idx_user_upload_status (status)
);

-- =========================================================
-- Fix mobile interaction tables for clean database initialization
-- Includes:
-- 1. user_artifact_upload
-- 2. artifact_like
-- 3. user_browse_history
-- 4. comment_like
-- 5. collation normalization for mobile interaction tables
-- =========================================================

-- 用户上传文物照片表
CREATE TABLE IF NOT EXISTS user_artifact_upload (
    object_id VARCHAR(36) PRIMARY KEY,
    user_id VARCHAR(36) NOT NULL,
    artifact_id VARCHAR(36) NOT NULL,
    image_path VARCHAR(1000) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    review_time DATETIME NULL,
    review_comment VARCHAR(500) NULL,
    is_deleted TINYINT NOT NULL DEFAULT 0,
    KEY idx_user_upload_user_time (user_id, create_time),
    KEY idx_user_upload_artifact (artifact_id),
    KEY idx_user_upload_status (status)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- 文物点赞表
CREATE TABLE IF NOT EXISTS artifact_like (
    object_id VARCHAR(36) PRIMARY KEY,
    user_id VARCHAR(36) NOT NULL,
    artifact_id VARCHAR(36) NOT NULL,
    create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_user_artifact_like (user_id, artifact_id),
    KEY idx_artifact_like_user (user_id),
    KEY idx_artifact_like_artifact (artifact_id),
    KEY idx_artifact_like_time (create_time)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- 浏览历史表
CREATE TABLE IF NOT EXISTS user_browse_history (
    object_id VARCHAR(36) PRIMARY KEY,
    user_id VARCHAR(36) NOT NULL,
    artifact_id VARCHAR(36) NOT NULL,
    browse_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_user_artifact_history (user_id, artifact_id),
    KEY idx_history_user_time (user_id, browse_time),
    KEY idx_history_artifact (artifact_id)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- 评论点赞表
CREATE TABLE IF NOT EXISTS comment_like (
    object_id VARCHAR(36) PRIMARY KEY,
    user_id VARCHAR(36) NOT NULL,
    comment_id VARCHAR(36) NOT NULL,
    create_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_user_comment_like (user_id, comment_id),
    KEY idx_comment_like_user (user_id),
    KEY idx_comment_like_comment (comment_id),
    KEY idx_comment_like_time (create_time)
) DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

-- =========================================================
-- Add missing columns when tables already exist
-- =========================================================

SET @sql_upload_update_time = (
    SELECT IF(
        COUNT(*) = 0,
        'ALTER TABLE user_artifact_upload ADD COLUMN update_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP',
        'SELECT 1'
    )
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name = 'user_artifact_upload'
      AND column_name = 'update_time'
);
PREPARE stmt_upload_update_time FROM @sql_upload_update_time;
EXECUTE stmt_upload_update_time;
DEALLOCATE PREPARE stmt_upload_update_time;

SET @sql_upload_is_deleted = (
    SELECT IF(
        COUNT(*) = 0,
        'ALTER TABLE user_artifact_upload ADD COLUMN is_deleted TINYINT NOT NULL DEFAULT 0',
        'SELECT 1'
    )
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name = 'user_artifact_upload'
      AND column_name = 'is_deleted'
);
PREPARE stmt_upload_is_deleted FROM @sql_upload_is_deleted;
EXECUTE stmt_upload_is_deleted;
DEALLOCATE PREPARE stmt_upload_is_deleted;

SET @sql_history_browse_time = (
    SELECT IF(
        COUNT(*) = 0,
        'ALTER TABLE user_browse_history ADD COLUMN browse_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP',
        'SELECT 1'
    )
    FROM information_schema.columns
    WHERE table_schema = DATABASE()
      AND table_name = 'user_browse_history'
      AND column_name = 'browse_time'
);
PREPARE stmt_history_browse_time FROM @sql_history_browse_time;
EXECUTE stmt_history_browse_time;
DEALLOCATE PREPARE stmt_history_browse_time;

-- =========================================================
-- Normalize collation for mobile interaction tables
-- This avoids Illegal mix of collations when joining artifact.object_id
-- =========================================================

ALTER TABLE user_favorite
    CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;

ALTER TABLE user_favorite_group
    CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;

ALTER TABLE user_privacy_setting
    CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;

ALTER TABLE user_artifact_upload
    CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;

ALTER TABLE artifact_like
    CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;

ALTER TABLE user_browse_history
    CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;

ALTER TABLE comment_like
    CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;