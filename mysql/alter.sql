-- =====================================================
-- 变更内容：给role表添加permissions字段
-- 字段说明：存储角色的权限标识列表，多个权限用英文逗号分隔
-- 执行说明：可多次安全执行，不会重复添加字段或修改现有数据
-- 依赖版本：MySQL 5.7+ / 8.0+（全版本兼容）
-- =====================================================

-- 1. 切换到目标数据库
USE admin_platform;

-- 2. 安全添加字段（先检查是否存在，避免重复执行报错）
SET @column_exists = (
    SELECT COUNT(*) 
    FROM information_schema.COLUMNS 
    WHERE TABLE_SCHEMA = 'admin_platform' 
      AND TABLE_NAME = 'role' 
      AND COLUMN_NAME = 'permissions'
);

SET @sql = IF(@column_exists = 0,
    "ALTER TABLE `role` ADD COLUMN `permissions` TEXT COMMENT '权限标识列表，多个权限用英文逗号分隔'",
    "SELECT '字段permissions已存在，无需重复添加' AS execution_result"
);

PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- 3. 初始化现有数据（将NULL值转为空字符串，避免应用程序空指针异常）
UPDATE `role` SET `permissions` = '' WHERE `permissions` IS NULL;

-- 4. 验证变更结果
SELECT '变更完成，当前role表结构如下：' AS verification_result;
DESCRIBE `role`;