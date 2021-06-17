package cn.edu.tsinghua.iginx.utils;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

public class HttpUtils
{

    public static String doPost(String url, Map<String, Double> map) {
        CloseableHttpClient httpClient = null;
        CloseableHttpResponse httpResponse = null;
        String result = "";
        httpClient = HttpClients.createDefault();
        HttpPost httpPost = new HttpPost(url);
        RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(350000)// 设置连接主机服务超时时间
                .setConnectionRequestTimeout(350000)// 设置连接请求超时时间
                .setSocketTimeout(600000)// 设置读取数据连接超时时间
                .build();
        httpPost.setConfig(requestConfig);
        httpPost.addHeader("Content-Type", "application/x-www-form-urlencoded");
        if (null != map && map.size() > 0) {
            StringBuilder ins = new StringBuilder();
            for (Map.Entry<String, Double> entry: map.entrySet())
            {
                ins.append(entry.getKey());
                ins.append("\2");
                ins.append(entry.getValue().toString());
                ins.append("\1");
            }
            if (ins.charAt(ins.length() - 1) == '\1')
                ins.deleteCharAt(ins.length() - 1);
            httpPost.setEntity(new StringEntity(ins.toString(), "UTF-8"));
        }
        try {
            httpResponse = httpClient.execute(httpPost);
        } catch (ClientProtocolException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (null != httpResponse) {
                try {
                    httpResponse.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (null != httpClient) {
                try {
                    httpClient.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return result;
    }

    public static String doPost(String url, List<String> list) {
        CloseableHttpClient httpClient = null;
        CloseableHttpResponse httpResponse = null;
        String result = "";
        httpClient = HttpClients.createDefault();
        HttpPost httpPost = new HttpPost(url);
        RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(350000)// 设置连接主机服务超时时间
                .setConnectionRequestTimeout(350000)// 设置连接请求超时时间
                .setSocketTimeout(600000)// 设置读取数据连接超时时间
                .build();
        httpPost.setConfig(requestConfig);
        httpPost.addHeader("Content-Type", "application/x-www-form-urlencoded");
        if (null != list && list.size() > 0) {
            StringBuilder ins = new StringBuilder(list.get(0));
            for (int i = 1; i < list.size(); i++)
                ins.append("\1" + list.get(i));
            httpPost.setEntity(new StringEntity(ins.toString(), "UTF-8"));
        }
        try {
            httpResponse = httpClient.execute(httpPost);
        } catch (ClientProtocolException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (null != httpResponse) {
                try {
                    httpResponse.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if (null != httpClient) {
                try {
                    httpClient.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return result;
    }
}