package com.kiona.ad_analysis.googleskan.model;

import com.alibaba.excel.annotation.ExcelProperty;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class DaySummary {
    @ExcelProperty("日期")
    private String day;
    @ExcelProperty("安装数量")
    private int install;
    @ExcelProperty("支付数量")
    private int purchase;
    @ExcelProperty("支付价值")
    private double purchaseValue;

}
