package com.myself.hadoop.mapreduce.mapjoin;

public class CustomerBean {
	
    
    private int custId;
    private String name;
    private String address;
    private String phone;
    
    public CustomerBean() {}
    
    public CustomerBean(int custId, String name, String address,
            String phone) {
        super();
        this.custId = custId;
        this.name = name;
        this.address = address;
        this.phone = phone;
    }

    
    
    public int getCustId() {
        return custId;
    }

    public String getName() {
        return name;
    }

    public String getAddress() {
        return address;
    }

    public String getPhone() {
        return phone;
    }


}
