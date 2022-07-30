package com.kiona.ad_analysis.googleskan.model;

import com.alibaba.excel.annotation.ExcelProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

@Data
@EqualsAndHashCode(callSuper = true)
@SuperBuilder
@NoArgsConstructor
public class DayCampaignSummary extends TimeSummary {
    @ExcelProperty("广告系列名称")
    private String campaign;
}
