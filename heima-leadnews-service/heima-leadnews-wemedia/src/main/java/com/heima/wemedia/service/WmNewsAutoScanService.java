package com.heima.wemedia.service;

import java.util.Date;

public interface WmNewsAutoScanService {

    /**
     * 自媒体文章审核
     * @param id  自媒体文章id
     * @param publishTime  自媒体文章发布时间publishTime
     */
    public void autoScanWmNews(Integer id, Date publishTime);
}
