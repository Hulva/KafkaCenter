package org.nesc.ec.bigdata.controller;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.nesc.ec.bigdata.common.BaseController;
import org.nesc.ec.bigdata.common.RestResponse;
import org.nesc.ec.bigdata.constant.BrokerConfig;
import org.nesc.ec.bigdata.constant.Constants;
import org.nesc.ec.bigdata.constant.TopicConfig;
import org.nesc.ec.bigdata.model.ClusterGroup;
import org.nesc.ec.bigdata.model.ClusterInfo;
import org.nesc.ec.bigdata.model.KafkaManagerBroker;
import org.nesc.ec.bigdata.service.AlertService;
import org.nesc.ec.bigdata.service.ClusterService;
import org.nesc.ec.bigdata.service.KafkaManagerService;
import org.nesc.ec.bigdata.service.MonitorService;
import org.nesc.ec.bigdata.service.TopicInfoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

@RestController
@RequestMapping("/manager")
public class KafkaManagerController extends BaseController{
    private static final Logger LOG = LoggerFactory.getLogger(KafkaManagerController.class);

    @Autowired
    TopicInfoService topicInfoService;
    @Autowired
    KafkaManagerService kafkaManagerService;
    @Autowired
    MonitorService monitorService;
    @Autowired
    ClusterService clusterService;
    @Autowired
    AlertService alertService;

    @GetMapping("/topic/list")
    public RestResponse topicList(@RequestParam("cluster") String clusterId) {
        try {
            return SUCCESS_DATA(kafkaManagerService.topicList(clusterId));
        } catch (Exception e) {
            LOG.error("Get Topic Config Error,",e);
        }
        return ERROR("Get Topic Config Error!Please check");
    }
    
    @PostMapping("/topic/config")
    public RestResponse topicConfig(@RequestBody Map<String,String> queryMap) {
        try {
            Map<String,JSONArray> map = new HashMap<>();
            String clusterId = queryMap.get(Constants.KeyStr.clusterId);
            String topicName = queryMap.get(Constants.KeyStr.TOPICNAME);
            map.put("config",kafkaManagerService.topicConfig(clusterId, topicName));
            return SUCCESS_DATA(map);
        } catch (Exception e) {
            LOG.error("Get Topic Config Error,",e);
        }
        return ERROR("Get Topic Config Error!Please check");
    }

    @PostMapping("/topic/summary")
    public RestResponse topicSummary(@RequestBody Map<String,String> queryMap) {
        try {
            String clusterId = queryMap.get(Constants.KeyStr.clusterId);
            String topicName = queryMap.get(Constants.KeyStr.TOPICNAME);
            return SUCCESS_DATA(kafkaManagerService.topicAndPartition(clusterId, topicName));
        } catch (Exception e) {
            LOG.error("Get Topic Summary Error,",e);
        }
        return ERROR("Get Topic Summary Error!Please check");
    }

    @PostMapping("/delete/topic")
    public RestResponse deleteTopic(@RequestBody Map<String,String> queryMap) {
        try {
            String clusterId = queryMap.get(Constants.KeyStr.clusterId);
            String topicName = queryMap.get(Constants.KeyStr.TOPICNAME);
            if(kafkaManagerService.deleteTopic(clusterId, topicName)) {
                return SUCCESS("Delete Topic Success");
            }
        } catch (Exception e) {
            LOG.error("Delete Topic Faild,",e);
        }
        return ERROR("Delete Topic Faild!");
    }

    @PostMapping("/topic/partition")
    public RestResponse addPartition(@RequestBody Map<String,String> queryMap) {
        try {
            String clusterId = queryMap.get(Constants.KeyStr.clusterId);
            String topicName = queryMap.get(Constants.KeyStr.TOPICNAME);
            String partition = queryMap.get(TopicConfig.PARTITION);
            String partitions = queryMap.get(TopicConfig.PARTITIONS);
            String oldPartition = queryMap.get(TopicConfig.OLDPARTITIONS);
            if(kafkaManagerService.addPartition(clusterId, topicName, Integer.parseInt(partition),partitions,Integer.parseInt(oldPartition))) {
                return SUCCESS("Add Partitions Success");
            }
        } catch (Exception e) {
            LOG.error("Add Partitions Faild,",e);
        }
        return ERROR("Add Partitions Faild!");
    }

    @PostMapping("/topic/desconfig")
    public RestResponse descrConfig(@RequestBody Map<String,String> queryMap) {
        try {
            String clusterId = queryMap.get(Constants.KeyStr.clusterId);
            String topicName = queryMap.get(Constants.KeyStr.TOPICNAME);
            return SUCCESS_DATA(kafkaManagerService.descConfig(clusterId, topicName));
        } catch (Exception e) {
            LOG.error("Describe Topic Config Faild,",e);
        }
        return ERROR("Describe Topic Config Faild!");
    }

    @PostMapping("/topic/desconfig/update")
    public RestResponse updateConfig(@RequestBody Map<String,Object> queryMap) {
        try {
            String clusterId = (String) queryMap.get(Constants.KeyStr.clusterId);
            String topicName = (String) queryMap.get(Constants.KeyStr.TOPICNAME);
            @SuppressWarnings("unchecked")
            Map<String,Object> map = (Map<String, Object>) queryMap.get(Constants.KeyStr.ENTRY);
            JSONObject obj = new JSONObject();
            map.forEach(obj::put);
            boolean flag = kafkaManagerService.updateConfig(topicName, clusterId, obj);
            if(flag) {
                return SUCCESS("Update Config Success");
            }
        } catch (Exception e) {
            LOG.error("Update Config Faild,",e) ;
        }
        return ERROR("Update Config Faild!");
    }

    @PostMapping("/topic/broker_list")
    public RestResponse descBrokerList(@RequestBody Map<String,Object> queryMap) {
        try {
            String clusterId = (String) queryMap.get(Constants.KeyStr.clusterId);
            JSONArray array  = kafkaManagerService.brokersList(clusterId);
            return SUCCESS_DATA(array);
        } catch (Exception e) {
            LOG.error("Desc Partition By Broker Faild,",e) ;
        }
        return ERROR("Desc Partition By Broker Faild");
    }

    @GetMapping(value = "/group")
    public RestResponse getClusterAllGroup(@RequestParam("cluster") String clusterId) {
		List<ClusterInfo> clusters = "-1".equalsIgnoreCase(clusterId) ? clusterService.getTotalData()
				: Arrays.asList(clusterService.selectById(Long.parseUnsignedLong(clusterId)));
        List<ClusterGroup> clusterGroups;
        try {
            clusterGroups = monitorService.listGroupsByCluster(clusters, false);
        } catch (Exception e) {
            LOG.error("List Group Error,message:", e);
            return ERROR("error");
        }
        return SUCCESS_DATA(clusterGroups);
    }

    @PostMapping(value = "/delete/group")
    public RestResponse deleteGroup(@RequestBody ClusterGroup clusterGroup) {
        try {
            Long clusterId = clusterGroup.getClusterID();
            String consumerGroup = clusterGroup.getConsummerGroup();
            String consumerApi = clusterGroup.getConsumereApi();
            kafkaManagerService.deleteGroup(String.valueOf(clusterId), consumerGroup, consumerApi);
            alertService.deleteAlertByClusterGroup(clusterGroup);
        } catch (Exception e) {
            LOG.error("delete group error" ,e);
            return ERROR("delete Group is error, please contact maintenance staff!");
        }
        return SUCCESS("delete group success");
    }

    @GetMapping("broker")
    public RestResponse getAllClusterBrokes(@RequestParam("cluster") String clusterId){
        List<KafkaManagerBroker> kafkaManagerBrokers;
        try {
            kafkaManagerBrokers = kafkaManagerService.getAllClusterBrokes(clusterId);
        } catch (Exception e) {
            LOG.error("getAllClusterBrokes Error,message:", e);
            return ERROR("error");
        }
        return SUCCESS_DATA(kafkaManagerBrokers);
    }
    
    @PostMapping("/group/rest/offset")
    public RestResponse restOffset(@RequestBody Map<String,String> map){
        try{
            String topic =  map.get(BrokerConfig.TOPIC);
            String group = map.get(BrokerConfig.GROUP);
            String clusterId = map.get(Constants.KeyStr.clusterId);
            if(kafkaManagerService.restOffset(clusterId,group,topic)){
                return SUCCESS("group rest offset success");
            }
        }catch (Exception e){
            LOG.error("group rest offset faild:", e);
            return ERROR("group rest offset faild");
        }
        return ERROR("group rest offset faild");
    }
    
    @PostMapping("/group/topic")
    public RestResponse getTopicByGroup(@RequestBody Map<String,String> map){
        try {
            String group = map.get(BrokerConfig.GROUP);
            String clusterId = map.get(Constants.KeyStr.clusterId);
            return SUCCESS_DATA(kafkaManagerService.getTopicByGroup(clusterId,group));
        }catch (Exception e){
            LOG.error("getTopicByGroup faild:", e);
            return ERROR("getTopicByGroup faild");
        }
    }
}
