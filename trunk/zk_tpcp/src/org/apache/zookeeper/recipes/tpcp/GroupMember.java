package org.apache.zookeeper.recipes.tpcp;

public class GroupMember {
    private String name;
    private TransactionGroup group;

    /**
     * Creates a new group member
     * @param name member name (just name)
     * @param group owning group
     */
    public GroupMember(String name, TransactionGroup group) {
        this.name = name;
        this.group = group;
    }

    /**
     * Get this member's name
     * @return
     */
    public String getName() {
        return name;
    }

    /**
     * Get this member's znode path
     * @return
     */
    public String getZnodePath() {
        return group.getGroupPath() + "/" + TransactionGroup.groupZnode + "/" + name;
    }

    /**
     * Get this member's group
     * @return
     */
    public TransactionGroup getGroup() {
        return group;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof GroupMember)) {
            return false;
        }

        GroupMember gm = (GroupMember) obj;

        return gm.group.equals(group) && gm.name.equals(name);
    }
}
