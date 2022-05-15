package com.kiona.ad_analysis.googleskan.model;

import com.alibaba.excel.annotation.ExcelProperty;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

/**
 * @author yangshuaichao
 * @date 2022/05/12 19:56
 * @description TODO
 */
@Data
@SuperBuilder
@NoArgsConstructor
public class Summary {
    @ExcelProperty("花费金额 (USD)")
    private double costMoney;
    @ExcelProperty("应用安装")
    private int install;
    @ExcelProperty("购物")
    private int purchase;
    @ExcelProperty("购物转化价值")
    private double purchaseValue;
}
