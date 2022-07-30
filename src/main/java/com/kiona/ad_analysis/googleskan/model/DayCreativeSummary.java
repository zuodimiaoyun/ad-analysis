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
public class DayCreativeSummary extends TimeSummary {
    @ExcelProperty("图片、视频和幻灯片")
    private String creative;
    @ExcelProperty("展示次数")
    private int display;
    @ExcelProperty("点击量（全部）")
    private int click;
}
