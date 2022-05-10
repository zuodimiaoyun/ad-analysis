package com.kiona.ad_analysis.v1;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class DayStat implements Stat{
    private String day;
    private int otherEventCount;
    @Builder.Default
    private int[] eventCounts = new int[64];

}
