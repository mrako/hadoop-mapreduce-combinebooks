package com.mrako.hadoop;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple CombineBooks.
 */
public class CombineBooksTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public CombineBooksTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( CombineBooksTest.class );
    }

    /**
     * Rigourous Test
     */
    public void testCombineBooks()
    {
        assertTrue( true );
    }
}
