



private Double gpsX;

    private Double gpsY;

    private String gpsXy; // 位置xy

    private Integer orderId; // 订单id

    private Integer isOnline;// 默认是在线。

    private List<Map<String, Object>> gpsXys = new ArrayList<>();// xys

    private String code; // 车辆编号

    private String boxCode; // 盒子编号

    private String boxMobile; // 盒子手机号

    private String battCode; // 电池盒子编号

    private String battMobile; // 电池盒子手机号

    private Float totalMileage; // 总里程数

    private Float orderMileage;// 订单里程数(米)

    private Float cellMileage; //该电池总里程数(米)

    private Float newBattMileage; //该电池运行里程数(米)

    private String zonuleIp; //车辆所处小区域定位码

    private Integer moonNumber; //卫星个数

    private String localIp; // 车辆所处基站定位码

    private String model; // 车型

    private String localCity; // 当前城市

    private Integer isLightting; // 闪灯 是否在闪灯0否1是

    private Integer isWhistle; // 鸣笛 是否在鸣笛 0否1是

    private Float inclines; // 姿态 电车与地面的角度0，180

    private Date locedChangeTime; // 电机锁的开或者关的时间点

    private Integer motorLockState; // 电机锁状态 0是锁 1 是开

    private Integer butteryState; // 电池是否通电 电池是否通电

    private Double voltage; //电压

    private Double margin; // 电量

    private Date openCloseTime; // 通电状态 电池通电，关电的时间

    private Integer isRuning; // 是否行车中

    private Integer isPolice; // 是否报警 1:正在报警 2.取消报警（触发条件：1.更换电池 2.地勤人员检查后）

    private Integer policeReason; // 报警原因 [1.电量过低 2.断电 3.未启动有平移 4.海拔过高 5.翻倒

    private Float speed; // 电车当前速度

    private Float gpsSpeed; // 与轮子无关的整车移动速度

    private Integer runStateTest; // 霍尔信号， 整车通电检查 0:正常 1：异常

    private Integer state; // 整车状态 0:未运营 1.运营中 2.维修中

    private Date updateTime; // 更新时间

    private Date beginTime; // 一键启动的开始时间

    private Date endTime; // 一键启动的结束时间

    private String reporterId; // 报警人

    private String handlerId; // 处理人

    private Integer isSwitch; //一键启动是否开启【0未开启，1开启】

    private Integer gSignal;//G网信号强度

    private Integer orderMins; //订单时长

    private Double battInBox;//终端内部电池电量

    private String gpsInfo; //原始的GPS坐标点

    private String boxIccid; //车辆终端SIM卡的ICCID

    private String version; //版本号

    private String controlVoltage; //控制器电压

    private Integer SpeedLimit; //是否限速【0没有限速，1限速】

    private Integer buttonState; //一键启动是否自动关闭【0自动关闭，1人为关闭】

    private String batteryVersion; //电池的版本号