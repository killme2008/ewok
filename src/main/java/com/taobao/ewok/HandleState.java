package com.taobao.ewok;

import java.io.Serializable;


/**
 * Ledger handle state
 * 
 * @author boyan(boyan@taobao.com)
 * 
 */
public class HandleState implements Serializable, Comparable<HandleState> {
    /**
     * 
     */
    private static final long serialVersionUID = 7478265040521083487L;
    // handle id
    public long id;
    // checkpoint
    public long checkpoint;


    public HandleState(long id, long checkpoint) {
        super();
        this.id = id;
        this.checkpoint = checkpoint;
    }


    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (id ^ (id >>> 32));
        return result;
    }


    public int compareTo(HandleState o) {
        if (o == null)
            return 1;
        if (this.id > o.id)
            return 1;
        else if (this.id < o.id)
            return -1;
        else
            return 0;
    }


    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        HandleState other = (HandleState) obj;
        if (id != other.id)
            return false;
        return true;
    }

}
