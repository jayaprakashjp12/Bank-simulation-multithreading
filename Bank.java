import java.io.FileWriter;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
//synchronised and threadsafe linkedlist
//fine grained locking
class LinkedList
{   
    //sentinal nodes
    Node head ;
    Node tail ;
    //constructor
    public LinkedList()
    {
        head = new Node(-1, -1);
        tail = new Node(9999999999999l,9999999999999l);
        head.next = tail;
    }
    class Node 
    { 
        long AccNum;
        long balance; 
        Node next; 
        Lock nodelock;
        // Constructor to create a new node 
        // Next is by default initialized 
        // as null 
        Node(long Accnum, long amount) 
        { 
            AccNum = Accnum; 
            balance = amount ; 
            nodelock = new ReentrantLock();
        } 
    } 
    public boolean insert(long accnum,long amount)
    {   
        long key = accnum;
        head.nodelock.lock();
        Node pred = head;
        try{
            Node curr = pred.next;
            curr.nodelock.lock();
            try{
                while(curr.AccNum < key)
                {
                    pred.nodelock.unlock();
                    pred = curr;
                    curr = curr.next;
                    curr.nodelock.lock();
                }
                if(curr.AccNum == key)return false;
                Node newnode = new Node(accnum,amount);
                newnode.next = curr;
                pred.next = newnode;
               // System.out.println("added new node ");
                return true;
            }finally{
                curr.nodelock.unlock();
            }

        }finally
        {
            pred.nodelock.unlock();
        }
    }
    public long delete(long accnum,long amount)
    {
        Node pred = null, curr = null;
        long key  = accnum; 
        head.nodelock.lock();
        try{
            pred = head;
            curr = pred.next;
            curr.nodelock.lock();
            try{
                while(curr.AccNum < key)
                {
                    pred.nodelock.unlock();
                    pred = curr; 
                    curr = curr.next;
                    curr.nodelock.lock();
                }
                if(curr.AccNum == key)
                {
                    pred.next = curr.next;
                    return curr.balance;
                }
                return -1;
            }finally
            {
                curr.nodelock.unlock();
            }
        }finally
        {
            pred.nodelock.unlock();
        }
    }
    public boolean modify(long accnum, long amount)
    {
        long key = accnum;
        head.nodelock.lock();
        Node pred = head;
        try{
            Node curr = pred.next;
            curr.nodelock.lock();
            try{
                while(curr.AccNum < key)
                {
                    pred.nodelock.unlock();
                    pred = curr;
                    curr = curr.next;
                    curr.nodelock.lock();
                }
                if(curr.AccNum == key)
                {
                    if(curr.balance+amount>=0)
                    {
                        curr.balance += amount;
                        return true;
                    }
                    else 
                    {
                        return false;
                    }
                }
                
                return false;
            }finally{
                curr.nodelock.unlock();
            }

        }finally
        {
            pred.nodelock.unlock();
        }
    }

    public void show_list(int i)
    {
        System.out.println("Linked list index :"+i);
        Node curr = head;
        while(curr.AccNum !=9999999999999l)
        {
            System.out.print("( "+ curr.AccNum +", "+curr.balance +") -> ");
            curr = curr.next;
        }
        System.out.println("( "+ curr.AccNum +", "+curr.balance +") ********************");

        System.out.println();

    }

}

class GNB{

    //array of linked list
    LinkedList[] AoL = new LinkedList[10];
    long  N = 1000000000;
    Branch [] branches = new Branch[10];
    Random rand = new Random();
    {
        try {
            FileWriter mywriter = new FileWriter("output.txt");
            mywriter.close();
        } catch (Exception e) {
            
            System.out.println("An error occurred.");
        }
    }

    public void simulate()
    {   
        for(int i=0;i<10;i++)
        {
            branches[i] = new Branch();
            AoL[i] = new LinkedList();
        }

        generate_requests();

    
        
        /*todo
     
        requests according to probability
        throughput
        hashing
    */
        


        for(int j=0;j<10;j++)
        {
            branches[j].shut();
        }

        for(int j=9;j<10;j++)
        {
            AoL[j].show_list(j);
        }
        
    } 

    public void generate_requests()
    {   
        HashMap<Integer,String> map = new HashMap<>();
        map.put(0,"add");
        map.put(1,"remove");
        map.put(2, "transferaccount");
        map.put(3, "deposit");
        map.put(4, "withdraw");
        map.put(5, "transferamount");
        int[] pref = new int[]{3,6,10,340,770,1000};
        
        //intialising
        for(int i=0;i<10;i++)
        {    
            for(int j=10000;j>=1;j--)
            {   
                long i_amount = rand.nextInt(100);
                branches[i].assign_request(map.get(0),(i*N+j),-1 , i_amount, i);
            }
        }

        //generating random requests.
        for(int i=0;i<10;i++)
        {
            for(int j=1;j<10000;j++)
            {
                int op = rand.nextInt(1000);
                for(int k=0;k<6;k++)
                {
                    if(pref[k]>op)
                    {
                        op = k;break;
                    }
                }
                int b = rand.nextInt(10), amnt = rand.nextInt(100);
                long ac1,ac2;
                ac1 = i*N+rand.nextInt(10000);ac2 =  b*N+rand.nextInt(10000);
                if(op == 0 )
                {
                    ac1 += 10000;
                }else if(op==2)
                {
                    ac2 += 10000;
                }
                
                branches[i].assign_request(map.get(op), ac1, ac2, amnt, i);
            }
        }
    }
    class Branch{
        //10 updaters having threads using thread pool.
        //For each request in this branch it assigns a thread which is free from thread pool.

        //intialsing thread pool
        ExecutorService pool  = Executors.newFixedThreadPool(10);
        
        public void assign_request(String op,long ac1,long ac2,long amount, long branch )
        {
            //System.out.println("fuckedup here");
            pool.submit(new Mthread(op,ac1,ac2,amount,branch));
        }
        public void shut()
        {
            pool.shutdown();
        }
    }

    public class Mthread extends Thread
    {
        String operation;
        long accnum1,accnum2,amount;
        long branch;

        public Mthread(String op,long ac1,long ac2,long amount,long branch)
        {
            this.operation=op;
            this.accnum1=ac1;
            this.accnum2=ac2;
            this.branch=branch;
            this.amount=amount;
        }

        @Override
        public void run()
        {   

           // System.out.println("current thread is : "+ currentThread().getId());
            if(operation=="add")
            {
                AoL [(int)branch].insert(accnum1, amount);
            }else if(operation == "remove")
            {
                AoL[(int)branch].delete(accnum1,amount);
            }else if(operation == "transferaccount")
            {
                long branch1 = accnum1/N,branch2 = accnum2/N;
                long amount_in_node = AoL[(int)branch1].delete(accnum1,amount);
                if(amount_in_node!=-1)
                {
                    AoL[(int)branch2].insert(accnum2, amount_in_node);
                }
            }else if(operation == "deposit")
            {
                AoL[(int)branch].modify(accnum1, amount);
            }else if(operation == "withdraw")
            {
                amount *=-1;
                AoL[(int)branch].modify(accnum1, amount);
            }
            else if(operation== "transferamount")
            {
                long branch1 = accnum1/N,branch2 = accnum2/N;
                amount*=-1;
                if(AoL[(int)branch1].modify(accnum1, amount))
                {
                    amount *=-1;
                    AoL[(int)branch2].modify(accnum2, amount);
                }
            }

        }
    }

}

public class Bank
{
    public static void main(String[] args)
    {
        GNB gnbbank = new GNB();
        gnbbank.simulate();   
    }
}
