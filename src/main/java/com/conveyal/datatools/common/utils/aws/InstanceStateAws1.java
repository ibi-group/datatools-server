package com.conveyal.datatools.common.utils.aws;

import software.amazon.awssdk.services.ec2.model.InstanceStateName;

import java.io.Serializable;

/**
 * Extracted from AWS SDK1 runtime because the SDK2 version can't be serialized for some reason.
 */
public class InstanceStateAws1 implements Serializable, Cloneable {
    private Integer code;
    private String name;

    public void setCode(Integer code) {
        this.code = code;
    }

    public Integer getCode() {
        return this.code;
    }

    public InstanceStateAws1 withCode(Integer code) {
        this.setCode(code);
        return this;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    public InstanceStateAws1 withName(String name) {
        this.setName(name);
        return this;
    }

    public void setName(InstanceStateName name) {
        this.withName(name);
    }

    public InstanceStateAws1 withName(InstanceStateName name) {
        this.name = name.toString();
        return this;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        if (this.getCode() != null) {
            sb.append("Code: ").append(this.getCode()).append(",");
        }

        if (this.getName() != null) {
            sb.append("Name: ").append(this.getName());
        }

        sb.append("}");
        return sb.toString();
    }

    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        } else if (obj == null) {
            return false;
        } else if (!(obj instanceof InstanceStateAws1)) {
            return false;
        } else {
            InstanceStateAws1 other = (InstanceStateAws1)obj;
            if (other.getCode() == null ^ this.getCode() == null) {
                return false;
            } else if (other.getCode() != null && !other.getCode().equals(this.getCode())) {
                return false;
            } else if (other.getName() == null ^ this.getName() == null) {
                return false;
            } else {
                return other.getName() == null || other.getName().equals(this.getName());
            }
        }
    }

    public int hashCode() {
        int prime = 31;
        int hashCode = 1;
        hashCode = prime * hashCode + (this.getCode() == null ? 0 : this.getCode().hashCode());
        hashCode = prime * hashCode + (this.getName() == null ? 0 : this.getName().hashCode());
        return hashCode;
    }

    public InstanceStateAws1 clone() {
        try {
            return (InstanceStateAws1)super.clone();
        } catch (CloneNotSupportedException e) {
            throw new IllegalStateException("Got a CloneNotSupportedException from Object.clone() even though we're Cloneable!", e);
        }
    }
}
