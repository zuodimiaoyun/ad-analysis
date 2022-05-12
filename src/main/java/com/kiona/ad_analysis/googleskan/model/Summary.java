package com.kiona.ad_analysis.googleskan.model;

import com.alibaba.excel.annotation.ExcelProperty;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.SuperBuilder;

/**
 * @author yangshuaichao
 * @date 2022/05/12 19:56
 * @description TODO
 */
@Data
@SuperBuilder
public class Summary {
  @ExcelProperty("安装数量")
  private int install;
  @ExcelProperty("支付数量")
  private int purchase;
  @ExcelProperty("支付价值")
  private double purchaseValue;
}
