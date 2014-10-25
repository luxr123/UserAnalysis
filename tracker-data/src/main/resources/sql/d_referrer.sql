/*
Navicat MySQL Data Transfer

Source Server         : 2.94
Source Server Version : 50173
Source Host           : 10.100.2.94:3306
Source Database       : hadoop

Target Server Type    : MYSQL
Target Server Version : 50173
File Encoding         : 65001

Date: 2014-06-03 15:15:18
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for d_referrer
-- ----------------------------
DROP TABLE IF EXISTS `d_referrer`;
CREATE TABLE `d_referrer` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `dataType` int(11) DEFAULT NULL,
  `refType` int(11) DEFAULT NULL,
  `domain` varchar(255) DEFAULT NULL,
  `domainId` int(11) DEFAULT NULL,
  `subDomain` varchar(255) DEFAULT NULL,
  `level` int(11) DEFAULT NULL,
  `name` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM AUTO_INCREMENT=55 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of d_referrer
-- ----------------------------
INSERT INTO `d_referrer` VALUES ('1', '1', '2', 'baidu.com', null, null, '1', '百度');
INSERT INTO `d_referrer` VALUES ('2', '1', '2', 'google.com', null, null, '1', '谷歌');
INSERT INTO `d_referrer` VALUES ('3', '1', '2', 'sogou.com', null, null, '1', '搜狗');
INSERT INTO `d_referrer` VALUES ('4', '1', '2', 'search.yahoo.com', null, null, '1', '雅虎搜索');
INSERT INTO `d_referrer` VALUES ('5', '1', '2', 'yahoo.cn', null, null, '1', 'yahoo');
INSERT INTO `d_referrer` VALUES ('6', '1', '2', 'soso.com', null, null, '1', '搜搜');
INSERT INTO `d_referrer` VALUES ('7', '1', '2', 'youdao.com', null, null, '1', '有道');
INSERT INTO `d_referrer` VALUES ('8', '1', '2', 'gougou.com', null, null, '1', '狗狗搜索');
INSERT INTO `d_referrer` VALUES ('9', '1', '2', 'bing.com', null, null, '1', 'Bing');
INSERT INTO `d_referrer` VALUES ('10', '1', '2', 'so.com', null, null, '1', '360搜索');
INSERT INTO `d_referrer` VALUES ('11', '1', '2', 'jike.com', null, null, '1', '即刻搜索');
INSERT INTO `d_referrer` VALUES ('12', '1', '2', 'qihoo.com', null, null, '1', '奇虎搜索');
INSERT INTO `d_referrer` VALUES ('13', '1', '2', 'etao.com', null, null, '1', '一淘搜索');
INSERT INTO `d_referrer` VALUES ('14', '1', '2', 'soku.com', null, null, '1', '搜酷');
INSERT INTO `d_referrer` VALUES ('15', '1', '2', 'easou.com', null, null, '1', '宜搜');
INSERT INTO `d_referrer` VALUES ('16', '1', '2', 'baidu.com', '1', 'news', '2', '百度新闻');
INSERT INTO `d_referrer` VALUES ('17', '1', '2', 'baidu.com', '1', 'tieba', '2', '百度贴吧');
INSERT INTO `d_referrer` VALUES ('18', '1', '2', 'baidu.com', '1', 'zhidao', '2', '百度知道');
INSERT INTO `d_referrer` VALUES ('19', '1', '2', 'baidu.com', '1', 'image', '2', '百度图片');
INSERT INTO `d_referrer` VALUES ('20', '1', '2', 'baidu.com', '1', 'video', '2', '百度视频');
INSERT INTO `d_referrer` VALUES ('21', '1', '2', 'baidu.com', '1', 'hi', '2', '百度空间');
INSERT INTO `d_referrer` VALUES ('22', '1', '2', 'baidu.com', '1', 'baike', '2', '百度百科');
INSERT INTO `d_referrer` VALUES ('23', '1', '2', 'baidu.com', '1', 'wenku', '2', '百度文库');
INSERT INTO `d_referrer` VALUES ('24', '1', '2', 'baidu.com', '1', 'opendata', '2', '百度开放平台');
INSERT INTO `d_referrer` VALUES ('25', '1', '2', 'baidu.com', '1', 'jingyan', '2', '百度经验');
INSERT INTO `d_referrer` VALUES ('27', '1', '2', 'sogou.com', '3', 'news', '2', '搜狗新闻');
INSERT INTO `d_referrer` VALUES ('28', '1', '2', 'sogou.com', '3', 'mp3', '2', '搜狗音乐');
INSERT INTO `d_referrer` VALUES ('29', '1', '2', 'sogou.com', '3', 'pic', '2', '搜狗图片');
INSERT INTO `d_referrer` VALUES ('32', '1', '2', 'sogou.com', '3', 'zhishi', '2', '搜狗知识');
INSERT INTO `d_referrer` VALUES ('33', '1', '2', 'sogou.com', '3', 'blogsearch', '2', '搜狗博客');
INSERT INTO `d_referrer` VALUES ('34', '1', '2', 'baidu.com', '1', '', '2', '百度网页搜索');
INSERT INTO `d_referrer` VALUES ('35', '1', '2', 'google.com', '2', '', '2', '谷歌网页搜索');
INSERT INTO `d_referrer` VALUES ('36', '1', '2', 'sogou.com', '3', '', '2', '搜狗网页搜索');
INSERT INTO `d_referrer` VALUES ('37', '1', '2', 'search.yahoo.com', '4', '', '2', '雅虎网页搜索');
INSERT INTO `d_referrer` VALUES ('38', '1', '2', 'yahoo.cn', '5', '', '2', 'yahoo');
INSERT INTO `d_referrer` VALUES ('39', '1', '2', 'soso.com', '6', '', '2', '搜搜网页搜索');
INSERT INTO `d_referrer` VALUES ('40', '1', '2', 'youdao.com', '7', '', '2', '有道网页搜索');
INSERT INTO `d_referrer` VALUES ('41', '1', '2', 'gougou.com', '8', '', '2', '狗狗网页搜索');
INSERT INTO `d_referrer` VALUES ('42', '1', '2', 'bing.com', '9', '', '2', '必应网页搜索');
INSERT INTO `d_referrer` VALUES ('43', '1', '2', 'so.com', '10', '', '2', '360网页搜索');
INSERT INTO `d_referrer` VALUES ('44', '1', '2', 'jike.com', '11', '', '2', '即刻网页搜索');
INSERT INTO `d_referrer` VALUES ('45', '1', '2', 'qihoo.com', '12', '', '2', '奇虎网页搜索');
INSERT INTO `d_referrer` VALUES ('46', '1', '2', 'etao.com', '13', '', '2', '一淘网页搜索');
INSERT INTO `d_referrer` VALUES ('47', '1', '2', 'soku.com', '14', '', '2', '搜酷网页搜索');
INSERT INTO `d_referrer` VALUES ('48', '1', '2', 'easou.com', '15', '', '2', '宜搜网页搜索');
INSERT INTO `d_referrer` VALUES ('53', '2', null, null, null, null, null, null);
INSERT INTO `d_referrer` VALUES ('54', '3', null, null, null, null, null, null);
