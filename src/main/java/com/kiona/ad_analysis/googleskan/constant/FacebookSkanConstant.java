package com.kiona.ad_analysis.googleskan.constant;

import java.util.regex.Pattern;

public class FacebookSkanConstant {
    public static final String HEADER_DAY = "^按日细分$";
    public static final String HEADER_CAMPAIGN = "广告系列名称";
    public static final String HEADER_OTHER_EVENT = "^(Unavailable_SKAdNetwork Conversions|不可用_SKAdNetwork 转化次数)$";

    public static final Pattern EVENT_HEADER_PATTERN = Pattern.compile("^(\\d+)_SKAdNetwork (Conversions|转化次数)$");
}
