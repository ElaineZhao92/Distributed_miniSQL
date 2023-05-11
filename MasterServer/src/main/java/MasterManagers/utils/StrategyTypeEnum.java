package MasterManagers.utils;

public enum StrategyTypeEnum {
    RECOVER(1),

    INVALID(2),

    ;

    /**
     * 策略类型编码
     */
    private final Integer code;

    StrategyTypeEnum(int code){
        this.code = code;
    }

}
