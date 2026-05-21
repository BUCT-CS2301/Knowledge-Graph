-- 后台管理子系统 - 数据库初始化脚本
-- 字符集: utf8mb4, 引擎: InnoDB

-- 确保数据库存在（首次启动时由 docker-compose 环境变量自动创建，此处做二次保障）
CREATE DATABASE IF NOT EXISTS admin_platform
    DEFAULT CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

USE admin_platform;

-- =====================================================
-- 1. 用户与认证模块
-- =====================================================

-- 全平台用户表
CREATE TABLE IF NOT EXISTS `user` (
    `object_id`       VARCHAR(36)    PRIMARY KEY,
    `username`        VARCHAR(50)    NOT NULL UNIQUE,
    `password_hash`   VARCHAR(255)   NOT NULL,
    `nickname`        VARCHAR(50),
    `email`           VARCHAR(100),
    `phone`           VARCHAR(20),
    `avatar`          VARCHAR(500),
    `user_type`       ENUM('ADMIN','KNOWLEDGE_SERVICE','MOBILE') NOT NULL DEFAULT 'ADMIN',
    `status`          ENUM('ENABLED','DISABLED') NOT NULL DEFAULT 'ENABLED',
    `last_login_time` DATETIME,
    `create_time`     DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `update_time`     DATETIME       ON UPDATE CURRENT_TIMESTAMP,
    `is_deleted`      TINYINT(1)     DEFAULT 0,
    INDEX `idx_user_type_status` (`user_type`, `status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 登录历史日志（选做）
CREATE TABLE IF NOT EXISTS `user_login_log` (
    `object_id`  VARCHAR(36) PRIMARY KEY,
    `user_id`    VARCHAR(36) NOT NULL,
    `login_time` DATETIME    NOT NULL,
    `ip_address` VARCHAR(45),
    `user_agent` VARCHAR(500),
    `result`     ENUM('SUCCESS','FAILURE') NOT NULL,
    FOREIGN KEY (`user_id`) REFERENCES `user`(`object_id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 用户行为记录（违规溯源）
CREATE TABLE IF NOT EXISTS `user_action_log` (
    `object_id`   VARCHAR(36) PRIMARY KEY,
    `user_id`     VARCHAR(36) NOT NULL,
    `action_type` VARCHAR(50) NOT NULL,
    `target_type` VARCHAR(50),
    `target_id`   VARCHAR(36),
    `detail`      TEXT,
    `create_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX `idx_user_time` (`user_id`, `create_time`),
    FOREIGN KEY (`user_id`) REFERENCES `user`(`object_id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =====================================================
-- 2. 角色与权限模块 (RBAC)
-- =====================================================

CREATE TABLE IF NOT EXISTS `role` (
    `object_id`   VARCHAR(36)  PRIMARY KEY,
    `role_name`   VARCHAR(50)  NOT NULL,
    `role_code`   VARCHAR(50)  NOT NULL UNIQUE,
    `description` VARCHAR(200),
    `is_system`   TINYINT(1)   DEFAULT 0,
    `create_time` DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `update_time` DATETIME     ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `permission` (
    `object_id`   VARCHAR(36)  PRIMARY KEY,
    `name`        VARCHAR(50)  NOT NULL,
    `code`        VARCHAR(100) NOT NULL UNIQUE,
    `type`        ENUM('MENU','BUTTON','API') NOT NULL,
    `parent_id`   VARCHAR(36),
    `sort`        INT          DEFAULT 0,
    `path`        VARCHAR(200),
    `icon`        VARCHAR(100),
    `create_time` DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX `idx_parent` (`parent_id`),
    FOREIGN KEY (`parent_id`) REFERENCES `permission`(`object_id`) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `user_role` (
    `user_id` VARCHAR(36) NOT NULL,
    `role_id` VARCHAR(36) NOT NULL,
    PRIMARY KEY (`user_id`, `role_id`),
    FOREIGN KEY (`user_id`) REFERENCES `user`(`object_id`) ON DELETE CASCADE,
    FOREIGN KEY (`role_id`) REFERENCES `role`(`object_id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `role_permission` (
    `role_id`       VARCHAR(36) NOT NULL,
    `permission_id` VARCHAR(36) NOT NULL,
    PRIMARY KEY (`role_id`, `permission_id`),
    FOREIGN KEY (`role_id`) REFERENCES `role`(`object_id`) ON DELETE CASCADE,
    FOREIGN KEY (`permission_id`) REFERENCES `permission`(`object_id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =====================================================
-- 3. 内容审核模块
-- =====================================================

CREATE TABLE IF NOT EXISTS `sensitive_word` (
    `object_id`   VARCHAR(36)  PRIMARY KEY,
    `word`        VARCHAR(100) NOT NULL UNIQUE,
    `create_time` DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `audit_rule` (
    `object_id`   VARCHAR(36) PRIMARY KEY,
    `rule_type`   VARCHAR(50) NOT NULL,
    `config_json` JSON        NOT NULL,
    `enabled`     TINYINT(1)  DEFAULT 1,
    `update_time` DATETIME    ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `audit_queue` (
    `object_id`         VARCHAR(36) PRIMARY KEY,
    `content_type`      ENUM('COMMENT','IMAGE','AUDIO','VIDEO','DYNAMIC') NOT NULL,
    `content_text`      TEXT,
    `content_url`       VARCHAR(1000),
    `author_id`         VARCHAR(36) NOT NULL,
    `auto_audit_result` ENUM('PENDING','PASS','REJECT','MANUAL') DEFAULT 'PENDING',
    `auto_audit_detail` VARCHAR(500),
    `status`            ENUM('PENDING','APPROVED','REJECTED') NOT NULL DEFAULT 'PENDING',
    `submit_time`       DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `audit_user_id`     VARCHAR(36),
    `audit_time`        DATETIME,
    `audit_remark`      VARCHAR(500),
    `reject_reason`     VARCHAR(500),
    INDEX `idx_status_submit` (`status`, `submit_time`),
    INDEX `idx_author` (`author_id`),
    FOREIGN KEY (`author_id`)    REFERENCES `user`(`object_id`) ON DELETE CASCADE,
    FOREIGN KEY (`audit_user_id`) REFERENCES `user`(`object_id`) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `audit_log` (
    `object_id`   VARCHAR(36) PRIMARY KEY,
    `queue_id`    VARCHAR(36),
    `action`      ENUM('APPROVE','REJECT','BATCH_APPROVE','BATCH_REJECT'),
    `operator_id` VARCHAR(36),
    `remark`      VARCHAR(500),
    `create_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (`queue_id`)    REFERENCES `audit_queue`(`object_id`) ON DELETE SET NULL,
    FOREIGN KEY (`operator_id`) REFERENCES `user`(`object_id`) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =====================================================
-- 4. 数据管理模块（文物与UGC）
-- =====================================================

CREATE TABLE IF NOT EXISTS `museum` (
    `object_id` VARCHAR(36)  PRIMARY KEY,
    `name`      VARCHAR(200) NOT NULL,
    `name_cn`   VARCHAR(200),
    `location`  VARCHAR(200),
    `website`   VARCHAR(500)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `artifact` (
    `object_id`        VARCHAR(36)  PRIMARY KEY,
    `title`            VARCHAR(500) NOT NULL,
    `period`           VARCHAR(200),
    `type`             VARCHAR(100),
    `material`         VARCHAR(200),
    `description`      TEXT,
    `dimensions`       VARCHAR(300),
    `museum_id`        VARCHAR(36),
    `detail_url`       VARCHAR(1000) NOT NULL,
    `image_url`        VARCHAR(1000) NOT NULL,
    `image_path`       VARCHAR(500),
    `credit_line`      VARCHAR(500),
    `accession_number` VARCHAR(100),
    `crawl_date`       DATE         NOT NULL,
    `create_time`      DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `update_time`      DATETIME     ON UPDATE CURRENT_TIMESTAMP,
    `is_deleted`       TINYINT(1)   DEFAULT 0,
    INDEX `idx_museum` (`museum_id`),
    INDEX `idx_type`   (`type`),
    INDEX `idx_period` (`period`),
    FOREIGN KEY (`museum_id`) REFERENCES `museum`(`object_id`) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `ugc_comment` (
    `object_id`    VARCHAR(36) PRIMARY KEY,
    `artifact_id`  VARCHAR(36),
    `user_id`      VARCHAR(36) NOT NULL,
    `parent_id`    VARCHAR(36),
    `content_text` TEXT        NOT NULL,
    `status`       ENUM('PENDING','APPROVED','REJECTED') DEFAULT 'PENDING',
    `likes`        INT         DEFAULT 0,
    `create_time`  DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (`artifact_id`) REFERENCES `artifact`(`object_id`) ON DELETE SET NULL,
    FOREIGN KEY (`user_id`)     REFERENCES `user`(`object_id`) ON DELETE CASCADE,
    FOREIGN KEY (`parent_id`)   REFERENCES `ugc_comment`(`object_id`) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `ugc_image` (
    `object_id`   VARCHAR(36)  PRIMARY KEY,
    `artifact_id` VARCHAR(36),
    `user_id`     VARCHAR(36)  NOT NULL,
    `image_url`   VARCHAR(1000) NOT NULL,
    `description` VARCHAR(500),
    `location`    VARCHAR(200),
    `status`      ENUM('PENDING','APPROVED','REJECTED') DEFAULT 'PENDING',
    `create_time` DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (`artifact_id`) REFERENCES `artifact`(`object_id`) ON DELETE SET NULL,
    FOREIGN KEY (`user_id`)     REFERENCES `user`(`object_id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `ugc_audio` (
    `object_id`   VARCHAR(36)  PRIMARY KEY,
    `artifact_id` VARCHAR(36),
    `user_id`     VARCHAR(36)  NOT NULL,
    `audio_url`   VARCHAR(1000) NOT NULL,
    `description` VARCHAR(500),
    `status`      ENUM('PENDING','APPROVED','REJECTED') DEFAULT 'PENDING',
    `create_time` DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (`artifact_id`) REFERENCES `artifact`(`object_id`) ON DELETE SET NULL,
    FOREIGN KEY (`user_id`)     REFERENCES `user`(`object_id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `ugc_video` (
    `object_id`   VARCHAR(36)  PRIMARY KEY,
    `artifact_id` VARCHAR(36),
    `user_id`     VARCHAR(36)  NOT NULL,
    `video_url`   VARCHAR(1000) NOT NULL,
    `description` VARCHAR(500),
    `status`      ENUM('PENDING','APPROVED','REJECTED') DEFAULT 'PENDING',
    `create_time` DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (`artifact_id`) REFERENCES `artifact`(`object_id`) ON DELETE SET NULL,
    FOREIGN KEY (`user_id`)     REFERENCES `user`(`object_id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `ugc_dynamic` (
    `object_id`   VARCHAR(36) PRIMARY KEY,
    `user_id`     VARCHAR(36) NOT NULL,
    `content_text` TEXT,
    `artifact_id` VARCHAR(36),
    `museum_id`   VARCHAR(36),
    `status`      ENUM('PENDING','APPROVED','REJECTED') DEFAULT 'PENDING',
    `create_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (`user_id`)     REFERENCES `user`(`object_id`) ON DELETE CASCADE,
    FOREIGN KEY (`artifact_id`) REFERENCES `artifact`(`object_id`) ON DELETE SET NULL,
    FOREIGN KEY (`museum_id`)   REFERENCES `museum`(`object_id`) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =====================================================
-- 5. 数据备份与恢复模块
-- =====================================================

CREATE TABLE IF NOT EXISTS `backup_record` (
    `object_id`   VARCHAR(36) PRIMARY KEY,
    `backup_type` ENUM('FULL','INCREMENTAL') NOT NULL,
    `file_path`   VARCHAR(500) NOT NULL,
    `file_size`   BIGINT,
    `status`      ENUM('SUCCESS','FAILED','IN_PROGRESS') NOT NULL,
    `description` VARCHAR(500),
    `operator_id` VARCHAR(36),
    `create_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX `idx_create_time` (`create_time`),
    FOREIGN KEY (`operator_id`) REFERENCES `user`(`object_id`) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `backup_schedule` (
    `object_id`            VARCHAR(36) PRIMARY KEY,
    `cron_expression`      VARCHAR(50) NOT NULL,
    `backup_type`          ENUM('FULL','INCREMENTAL') NOT NULL,
    `enabled`              TINYINT(1) DEFAULT 1,
    `description`          VARCHAR(500),
    `last_execution_time`  DATETIME,
    `next_execution_time`  DATETIME,
    `create_time`          DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `update_time`          DATETIME ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =====================================================
-- 6. 日志管理模块
-- =====================================================

CREATE TABLE IF NOT EXISTS `operation_log` (
    `object_id`   VARCHAR(36) PRIMARY KEY,
    `user_id`     VARCHAR(36),
    `module`      VARCHAR(50) NOT NULL,
    `action`      VARCHAR(50) NOT NULL,
    `target_type` VARCHAR(50),
    `target_id`   VARCHAR(36),
    `detail`      JSON,
    `ip_address`  VARCHAR(45),
    `create_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX `idx_time` (`create_time`),
    INDEX `idx_user` (`user_id`),
    FOREIGN KEY (`user_id`) REFERENCES `user`(`object_id`) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `system_log` (
    `object_id`   VARCHAR(36) PRIMARY KEY,
    `level`       ENUM('DEBUG','INFO','WARN','ERROR') NOT NULL,
    `module`      VARCHAR(50),
    `message`     TEXT NOT NULL,
    `exception`   TEXT,
    `create_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX `idx_level_time` (`level`, `create_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `security_log` (
    `object_id`   VARCHAR(36) PRIMARY KEY,
    `user_id`     VARCHAR(36),
    `event_type`  VARCHAR(50) NOT NULL,
    `detail`      VARCHAR(500),
    `ip_address`  VARCHAR(45),
    `create_time` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (`user_id`) REFERENCES `user`(`object_id`) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- =====================================================
-- 7. 系统配置模块
-- =====================================================

CREATE TABLE IF NOT EXISTS `sys_config` (
    `object_id`    VARCHAR(36)  PRIMARY KEY,
    `config_key`   VARCHAR(100) NOT NULL UNIQUE,
    `config_value` TEXT         NOT NULL,
    `description`  VARCHAR(200),
    `update_time`  DATETIME     ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `sys_announcement` (
    `object_id`      VARCHAR(36)  PRIMARY KEY,
    `title`          VARCHAR(200) NOT NULL,
    `content`        TEXT         NOT NULL,
    `status`         ENUM('ACTIVE','INACTIVE') DEFAULT 'ACTIVE',
    `publish_time`   DATETIME,
    `expire_time`    DATETIME,
    `create_user_id` VARCHAR(36),
    FOREIGN KEY (`create_user_id`) REFERENCES `user`(`object_id`) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;