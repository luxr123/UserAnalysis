/*
Navicat MySQL Data Transfer

Source Server         : 2.94
Source Server Version : 50173
Source Host           : 10.100.2.94:3306
Source Database       : hadoop

Target Server Type    : MYSQL
Target Server Version : 50173
File Encoding         : 65001

Date: 2014-06-25 17:38:06
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for d_page
-- ----------------------------
DROP TABLE IF EXISTS `d_page`;
CREATE TABLE `d_page` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `webId` int(255) DEFAULT NULL,
  `urlType` int(255) DEFAULT NULL,
  `pageDesc` varchar(255) DEFAULT NULL,
  `url` varchar(255) DEFAULT NULL,
  `keyword` varchar(255) DEFAULT NULL,
  `isUsed` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=MyISAM AUTO_INCREMENT=57 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of d_page
-- ----------------------------
INSERT INTO `d_page` VALUES ('1', '1', '1', '其他页面', 'other url', 'other', '1');
INSERT INTO `d_page` VALUES ('2', '1', '1', '网站首页', 'http://jingying', 'home', '1');
INSERT INTO `d_page` VALUES ('3', '1', '1', '网站首页', 'http://jingying/index.php', 'index.php', '1');
INSERT INTO `d_page` VALUES ('4', '1', '1', '忘记密码', 'http://jingying/index.php?act=emailpassword', 'index.php?act=emailpassword', '1');
INSERT INTO `d_page` VALUES ('5', '1', '1', '猎头-认证页面', 'http://jingying/spy/spyauth.php', 'spy/spyauth.php', '1');
INSERT INTO `d_page` VALUES ('6', '1', '1', '猎头-实名认证', 'http://jingying/spy/spyauth.php?act=rnauth', 'spy/spyauth.php?act=rnauth', '1');
INSERT INTO `d_page` VALUES ('7', '1', '1', '猎头-企业认证', 'http://jingying/spy/spyauth.php?act=comauth', 'spy/spyauth.php?act=comauth', '1');
INSERT INTO `d_page` VALUES ('8', '1', '1', '猎头-中介认证', 'http://jingying/spy/spyauth.php?act=medauth', 'spy/spyauth.php?act=medauth', '1');
INSERT INTO `d_page` VALUES ('9', '1', '1', '猎头-签约认证', 'http://jingying/spy/spyauth.php?act=sigauth', 'spy/spyauth.php?act=sigauth', '1');
INSERT INTO `d_page` VALUES ('10', '1', '1', '猎头-修改密码', 'http://jingying/spy/setpassword.php', 'spy/setpassword.php', '1');
INSERT INTO `d_page` VALUES ('11', '1', '1', '猎头-找精英', 'http://jingying/spy/searchmanager.php', 'spy/searchmanager.php', '1');
INSERT INTO `d_page` VALUES ('12', '1', '2', '猎头-查看精英', 'http://jingying/manager/cv.php?act=showCv', 'manager/cv.php?act=showCv', '1');
INSERT INTO `d_page` VALUES ('13', '1', '2', '猎头-人脉管理', 'http://jingying/spy/mymanager.php', 'spy/mymanager.php', '1');
INSERT INTO `d_page` VALUES ('14', '1', '1', '猎头-搜索case', 'http://jingying/spy/searchcase.php', 'spy/searchcase.php', '1');
INSERT INTO `d_page` VALUES ('15', '1', '2', '猎头-查看case', 'http://jingying/spy/casemanage.php?act=detail&casetype=foxspycase', 'spy/casemanage.php?act=detail&casetype=spycase', '1');
INSERT INTO `d_page` VALUES ('16', '1', '1', '猎头-case发布', 'http://jingying/spy/casemanage.php?act=addcase', 'spy/casemanage.php?act=addcase', '1');
INSERT INTO `d_page` VALUES ('17', '1', '1', '猎头-查看发布中的case', 'http://jingying/spy/casemanage.php?type=spycase&status=01', 'spy/casemanage.php?type=spycase&status=01', '1');
INSERT INTO `d_page` VALUES ('18', '1', '1', '猎头-查看编辑中的case', 'http://jingying/spy/casemanage.php?type=spycase&status=02', 'spy/casemanage.php?type=spycase&status=02', '1');
INSERT INTO `d_page` VALUES ('19', '1', '1', '猎头-查看申请的企业委托', 'http://jingying/spy/casemanage.php?type=hrcase&status=01', 'spy/casemanage.php?type=hrcase&status=01', '1');
INSERT INTO `d_page` VALUES ('20', '1', '2', '猎头-系统消息', 'http://jingying/spy/privateletter.php?receivetype=sys', 'spy/privateletter.php?receivetype=sys', '1');
INSERT INTO `d_page` VALUES ('21', '1', '2', '猎头-与经理人消息', 'http://jingying/spy/privateletter.php?receivetype=mgr', 'spy/privateletter.php?receivetype=mgr', '1');
INSERT INTO `d_page` VALUES ('22', '1', '1', '经理人-修改Profile', 'http://jingying/manager/managerprofile.php', 'manager/managerprofile.php', '1');
INSERT INTO `d_page` VALUES ('23', '1', '2', '经理人-显示profile简历', 'http://jingying/manager/managerprofile.php?act=showcv', 'manager/managerprofile.php?act=showcv', '1');
INSERT INTO `d_page` VALUES ('24', '1', '1', '经理人-修改密码', 'http://jingying/manager/setpassword.php', 'manager/setpassword.php', '1');
INSERT INTO `d_page` VALUES ('25', '1', '1', '经理人-找猎头', 'http://jingying/manager/searchspy.php', 'manager/searchspy.php', '1');
INSERT INTO `d_page` VALUES ('26', '1', '2', '经理人-查看猎头信息', 'http://jingying/spy/spyinfo.php', 'spy/spyinfo.php', '1');
INSERT INTO `d_page` VALUES ('27', '1', '2', '经理人-人脉管理', 'http://jingying/manager/myspy.php', 'manager/searchcase.php', '1');
INSERT INTO `d_page` VALUES ('28', '1', '1', '经理人-搜索case', 'http://jingying/manager/searchcase.php', 'manager/searchcase.php', '1');
INSERT INTO `d_page` VALUES ('29', '1', '2', '经理人-查看case', 'http://jingying/manager/casemanage.php?act=detail&casetype=foxspycase', 'manager/casemanage.php?act=detail&casetype=foxspycase', '1');
INSERT INTO `d_page` VALUES ('30', '1', '2', '经理人-与猎头交互消息', 'http://jingying/manager/privateletter.php?receivetype=spy', 'manager/privateletter.php?receivetype=spy', '1');
