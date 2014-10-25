/*
Navicat MySQL Data Transfer

Source Server         : 2.94
Source Server Version : 50173
Source Host           : 10.100.2.94:3306
Source Database       : hadoop

Target Server Type    : MYSQL
Target Server Version : 50173
File Encoding         : 65001

Date: 2014-05-26 12:23:55
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for d_referrer_search
-- ----------------------------
DROP TABLE IF EXISTS `d_referrer_search`;
CREATE TABLE `d_referrer_search` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `domain` varchar(255) DEFAULT NULL,
  `domainId` int(11) DEFAULT NULL,
  `subDomain` varchar(255) DEFAULT NULL,
  `level` int(11) DEFAULT NULL,
  `name` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM AUTO_INCREMENT=49 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of d_referrer_search
-- ----------------------------
INSERT INTO `d_referrer_search` VALUES ('1', 'baidu.com', null, null, '1', '百度');
INSERT INTO `d_referrer_search` VALUES ('2', 'google.com', null, null, '1', '谷歌');
INSERT INTO `d_referrer_search` VALUES ('3', 'sogou.com', null, null, '1', '搜狗');
INSERT INTO `d_referrer_search` VALUES ('4', 'search.yahoo.com', null, null, '1', '雅虎搜索');
INSERT INTO `d_referrer_search` VALUES ('5', 'yahoo.cn', null, null, '1', 'yahoo');
INSERT INTO `d_referrer_search` VALUES ('6', 'soso.com', null, null, '1', '搜搜');
INSERT INTO `d_referrer_search` VALUES ('7', 'youdao.com', null, null, '1', '有道');
INSERT INTO `d_referrer_search` VALUES ('8', 'gougou.com', null, null, '1', '狗狗搜索');
INSERT INTO `d_referrer_search` VALUES ('9', 'bing.com', null, null, '1', 'Bing');
INSERT INTO `d_referrer_search` VALUES ('10', 'so.com', null, null, '1', '360搜索');
INSERT INTO `d_referrer_search` VALUES ('11', 'jike.com', null, null, '1', '即刻搜索');
INSERT INTO `d_referrer_search` VALUES ('12', 'qihoo.com', null, null, '1', '奇虎搜索');
INSERT INTO `d_referrer_search` VALUES ('13', 'etao.com', null, null, '1', '一淘搜索');
INSERT INTO `d_referrer_search` VALUES ('14', 'soku.com', null, null, '1', '搜酷');
INSERT INTO `d_referrer_search` VALUES ('15', 'easou.com', null, null, '1', '宜搜');
INSERT INTO `d_referrer_search` VALUES ('16', 'baidu.com', '1', 'news', '2', '百度新闻');
INSERT INTO `d_referrer_search` VALUES ('17', 'baidu.com', '1', 'tieba', '2', '百度贴吧');
INSERT INTO `d_referrer_search` VALUES ('18', 'baidu.com', '1', 'zhidao', '2', '百度知道');
INSERT INTO `d_referrer_search` VALUES ('19', 'baidu.com', '1', 'image', '2', '百度图片');
INSERT INTO `d_referrer_search` VALUES ('20', 'baidu.com', '1', 'video', '2', '百度视频');
INSERT INTO `d_referrer_search` VALUES ('21', 'baidu.com', '1', 'hi', '2', '百度空间');
INSERT INTO `d_referrer_search` VALUES ('22', 'baidu.com', '1', 'baike', '2', '百度百科');
INSERT INTO `d_referrer_search` VALUES ('23', 'baidu.com', '1', 'wenku', '2', '百度文库');
INSERT INTO `d_referrer_search` VALUES ('24', 'baidu.com', '1', 'opendata', '2', '百度开放平台');
INSERT INTO `d_referrer_search` VALUES ('25', 'baidu.com', '1', 'jingyan', '2', '百度经验');
INSERT INTO `d_referrer_search` VALUES ('27', 'sogou.com', '3', 'news', '2', '搜狗新闻');
INSERT INTO `d_referrer_search` VALUES ('28', 'sogou.com', '3', 'mp3', '2', '搜狗音乐');
INSERT INTO `d_referrer_search` VALUES ('29', 'sogou.com', '3', 'pic', '2', '搜狗图片');
INSERT INTO `d_referrer_search` VALUES ('32', 'sogou.com', '3', 'zhishi', '2', '搜狗知识');
INSERT INTO `d_referrer_search` VALUES ('33', 'sogou.com', '3', 'blogsearch', '2', '搜狗博客');
INSERT INTO `d_referrer_search` VALUES ('34', 'baidu.com', '1', '', '2', '百度网页搜索');
INSERT INTO `d_referrer_search` VALUES ('35', 'google.com', '2', '', '2', '谷歌网页搜索');
INSERT INTO `d_referrer_search` VALUES ('36', 'sogou.com', '3', '', '2', '搜狗网页搜索');
INSERT INTO `d_referrer_search` VALUES ('37', 'search.yahoo.com', '4', '', '2', '雅虎网页搜索');
INSERT INTO `d_referrer_search` VALUES ('38', 'yahoo.cn', '5', '', '2', 'yahoo');
INSERT INTO `d_referrer_search` VALUES ('39', 'soso.com', '6', '', '2', '搜搜网页搜索');
INSERT INTO `d_referrer_search` VALUES ('40', 'youdao.com', '7', '', '2', '有道网页搜索');
INSERT INTO `d_referrer_search` VALUES ('41', 'gougou.com', '8', '', '2', '狗狗网页搜索');
INSERT INTO `d_referrer_search` VALUES ('42', 'bing.com', '9', '', '2', '必应网页搜索');
INSERT INTO `d_referrer_search` VALUES ('43', 'so.com', '10', '', '2', '360网页搜索');
INSERT INTO `d_referrer_search` VALUES ('44', 'jike.com', '11', '', '2', '即刻网页搜索');
INSERT INTO `d_referrer_search` VALUES ('45', 'qihoo.com', '12', '', '2', '奇虎网页搜索');
INSERT INTO `d_referrer_search` VALUES ('46', 'etao.com', '13', '', '2', '一淘网页搜索');
INSERT INTO `d_referrer_search` VALUES ('47', 'soku.com', '14', '', '2', '搜酷网页搜索');
INSERT INTO `d_referrer_search` VALUES ('48', 'easou.com', '15', '', '2', '宜搜网页搜索');
