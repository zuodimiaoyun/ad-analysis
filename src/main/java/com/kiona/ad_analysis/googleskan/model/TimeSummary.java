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
public class TimeSummary extends Summary {
    @ExcelProperty(index = 0, order = 0)
    private String day;
    @ExcelProperty(value = "周", order = 1)
    private String week;
    @ExcelProperty(value = "月", order = 2)
    private String month;
}
