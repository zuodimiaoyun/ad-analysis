package com.kiona.ad_analysis.googleskan.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class DayCampaignStat implements Stat{
    private String campaign;
    private String day;
    private int otherEventCount;
    @Builder.Default
    private int[] eventCounts = new int[64];
}
