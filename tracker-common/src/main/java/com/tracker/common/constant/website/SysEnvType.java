package com.tracker.common.constant.website;

/**
 * 系统环境表
 * @author jason.hua
 *
 */
public enum SysEnvType {
	BROWSER(1), //浏览器
	OS(2), //操作系统
	COLOR_DEPTH(3), //屏幕颜色
	COOKIE_ENABLED(4), //是否支持cookie
	LANGUAGE(5), //语言环境
	SCREEN(6); //屏幕分辩率
	
	
	private int value;
	
	private SysEnvType(int value){
		this.value = value;
	}

	public int getValue() {
		return value;
	}

	public void setValue(int value) {
		this.value = value;
	}
	
	public static SysEnvType valueOf(int value){
		switch(value){
			case 1:
				return SysEnvType.BROWSER;
			case 2:
				return SysEnvType.OS;
			case 3:
				return SysEnvType.COLOR_DEPTH;
			case 4:
				return SysEnvType.COOKIE_ENABLED;
			case 5:
				return SysEnvType.LANGUAGE;
			case 6:
				return SysEnvType.SCREEN;
			case 7:
				return SysEnvType.SCREEN;
			default:
				return null;
		}
	}
}
