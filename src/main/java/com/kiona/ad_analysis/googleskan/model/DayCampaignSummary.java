package com.kiona.ad_analysis.googleskan.model;

import com.alibaba.excel.annotation.ExcelProperty;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.SuperBuilder;

@Data
@EqualsAndHashCode(callSuper = true)
@SuperBuilder
public class DayCampaignSummary extends DaySummary{
    @ExcelProperty("Campaign")
    private String campaign;
}
