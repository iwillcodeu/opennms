//
// This file is part of the OpenNMS(R) Application.
//
// OpenNMS(R) is Copyright (C) 2002-2003 The OpenNMS Group, Inc. All rights reserved.
// OpenNMS(R) is a derivative work, containing both original code, included code and modified
// code that was published under the GNU General Public License. Copyrights for modified
// and included code are below.
//
// OpenNMS(R) is a registered trademark of The OpenNMS Group, Inc.
//
// Modifications:
//
// 2004 May 05: Switch from SocketChannel to Socket with connection timeout.
// 2003 Jul 21: Explicitly closed socket.
// 2003 Jul 18: Enabled retries for monitors.
// 2003 Jan 31: Added the ability to imbed RRA information in poller packages.
// 2003 Jan 31: Cleaned up some unused imports.
// 2003 Jan 29: Added response times to certain monitors.
// 2002 Nov 14: Used non-blocking I/O socket channel classes.
//
// Original code base Copyright (C) 1999-2001 Oculan Corp. All rights reserved.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
//
// For more information contact:
//      OpenNMS Licensing <license@opennms.org>
//      http://www.opennms.org/
//      http://www.opennms.com/
//
// Tab Size = 8
//

package org.opennms.netmgt.poller;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NoRouteToHostException;
import java.net.Socket;
import java.util.Map;

import org.apache.log4j.Category;
import org.apache.log4j.Priority;
import org.opennms.core.utils.ThreadCategory;
import org.opennms.netmgt.utils.ParameterMap;

/**
 * <P>
 * This class is designed to be used by the service poller framework to test the availability
 * of IIOP running on a Domino server service on. The class implements the ServiceMonitor
 * interface that allows it to be used along with other plug-ins by the service poller
 * framework.
 * </P>
 * 
 * @author <A HREF="mailto:tarus@opennms.org">Tarus Balog </A>
 * @author <A HREF="mike@opennms.org">Mike </A>
 * @author <A HREF="weave@oculan.com">Weave </A>
 * @author <A HREF="http://www.opennms.org/">OpenNMS </A>
 *  
 */
final class DominoIIOPMonitor extends IPv4Monitor {

    /**
     * Default port.
     */
    private static final int DEFAULT_PORT = 63148;

    /**
     * Default port of where to find the IOR via HTTP
     */
    private static final int DEFAULT_IORPORT = 80;

    /**
     * Default retries.
     */
    private static final int DEFAULT_RETRY = 3;

    /**
     * Default timeout. Specifies how long (in milliseconds) to block waiting for data from the
     * monitored interface.
     */
    private static final int DEFAULT_TIMEOUT = 3000; // 3 second timeout on read()

    /**
     * Poll the specified address for service availability.
     * 
     * During the poll an attempt is made to connect on the specified port. If the connection
     * request is successful, the banner line generated by the interface is parsed and if the
     * banner text indicates that we are talking to Provided that the interface's response is
     * valid we set the service status to SERVICE_AVAILABLE and return.
     * 
     * @param iface
     *            The network interface to test the service on.
     * @param parameters
     *            The package parameters (timeout, retry, and others) to be used for this poll.
     * 
     * @return The availibility of the interface and if a transition event should be supressed.
     * 
     * @throws java.lang.RuntimeException
     *             Thrown if the interface experiences errors during the poll.
     */
    public int poll(NetworkInterface iface, Map parameters, org.opennms.netmgt.config.poller.Package pkg) {
        //
        // Process parameters
        //
        Category log = ThreadCategory.getInstance(getClass());

        //
        // Get interface address from NetworkInterface
        //
        if (iface.getType() != NetworkInterface.TYPE_IPV4)
                throw new NetworkInterfaceNotSupportedException("Unsupported interface type, only TYPE_IPV4 currently supported");

        int retry = ParameterMap.getKeyedInteger(parameters, "retry", DEFAULT_RETRY);
        int timeout = ParameterMap.getKeyedInteger(parameters, "timeout", DEFAULT_TIMEOUT);
        int IORport = ParameterMap.getKeyedInteger(parameters, "ior-port", DEFAULT_IORPORT);

        // Port
        //
        int port = ParameterMap.getKeyedInteger(parameters, "port", DEFAULT_PORT);

        // Get the address instance.
        //
        InetAddress ipv4Addr = (InetAddress) iface.getAddress();

        if (log.isDebugEnabled())
                log.debug("poll: address = " + ipv4Addr.getHostAddress() + ", port = " + port + ", timeout = " + timeout + ", retry = " + retry);

        int serviceStatus = SERVICE_UNAVAILABLE;

        // THIS SHOULD WORK BUT IT DOESN'T, AND I DON'T KNOW WHY, SO WE HAVE TO DO IT THE HARD
        // WAY...
        /*
         * Session session = NotesFactory.createSession(hostname);
         */

        // Lets first try to the the IOR via HTTP, if we can't get that then any other process
        // that can
        // do it the right way won't be able to connect anyway
        //
        try {
            String IOR = retrieveIORText(ipv4Addr.getHostAddress(), IORport);
        } catch (FileNotFoundException e) {
            // This is an expected exception
            //
            if (log.isDebugEnabled()) log.debug("DominoIIOPMonitor: failed to get the corba IOR from " + ipv4Addr, e);
            return serviceStatus;
        }

        catch (Exception e) {
            if (log.isDebugEnabled()) log.debug("DominoIIOPMonitor: failed to get the corba IOR from " + ipv4Addr, e);
            return serviceStatus;
        }

        /*
         * THIS IS THE WAY WE SHOULD BE CONNECTING TO THE DOMINO IIOP STUFF, BUT SINCE IT 'NO
         * WORKY' LEAVE IT OUT
         */
        /*
         * // Initialize the ORB in NCSO.jar. java.util.Properties ibm_props = new
         * java.util.Properties(); ibm_props.put("org.omg.CORBA.ORBClass",
         * "com.ibm.CORBA.iiop.ORB"); org.omg.CORBA.ORB ibm_orb = org.omg.CORBA.ORB.init(args,
         * ibm_props); // Bind to initial object using IOR org.omg.CORBA.Object ibm_obj =
         * ibm_orb.string_to_object(IOR); lotus.domino.corba.IObjectServer rObjectServer =
         * lotus.domino.corba.IObjectServerHelper.narrow(ibm_obj);
         */

        //SO LETS DO IT THE OLD FASHIONED WAY
        for (int attempts = 0; attempts <= retry && serviceStatus != SERVICE_AVAILABLE; attempts++) {
            Socket socket = null;
            try {
                //
                // create a connected socket
                //
                socket = new Socket();
                socket.connect(new InetSocketAddress(ipv4Addr, port), timeout);
                socket.setSoTimeout(timeout);
                log.debug("DominoIIOPMonitor: connected to host: " + ipv4Addr + " on port: " + port);

                //got here so its up...
                serviceStatus = SERVICE_AVAILABLE;
            } catch (NoRouteToHostException e) {
                e.fillInStackTrace();
                if (log.isEnabledFor(Priority.WARN))
                        log.warn("DominoIIOPMonitor: No route to host exception for address " + ipv4Addr.getHostAddress(), e);
                break; // Break out of for(;;)
            } catch (InterruptedIOException e) {
                log.debug("DominoIIOPMonitor: did not connect to host within timeout: " + timeout + " attempt: " + attempts);
            } catch (ConnectException e) {
                // Connection refused. Continue to retry.
                //
                e.fillInStackTrace();
                if (log.isDebugEnabled()) log.debug("DominoIIOPMonitor: Connection exception for address: " + ipv4Addr, e);
            } catch (IOException e) {
                // Ignore
                e.fillInStackTrace();
                if (log.isDebugEnabled()) log.debug("DominoIIOPMonitor: IOException while polling address: " + ipv4Addr, e);
            } finally {
                try {
                    // Close the socket
                    if (socket != null) socket.close();
                } catch (IOException e) {
                    e.fillInStackTrace();
                    if (log.isDebugEnabled()) log.debug("DominoIIOPMonitor: Error closing socket.", e);
                }
            }
        }

        //
        // return the status of the service
        //
        return serviceStatus;
    }

    /**
     * Method used to retrieve the IOR string from the Domino server.
     * 
     * @param host
     *            the host name which has the IOR
     * @param port
     *            the port to find the IOR via HTTP
     */
    private String retrieveIORText(String host, int port) throws IOException {
        String IOR = "";
        java.net.URL u = new java.net.URL("http://" + host + ":" + port + "/diiop_ior.txt");
        java.io.InputStream is = u.openStream();
        java.io.BufferedReader dis = new java.io.BufferedReader(new java.io.InputStreamReader(is));
        boolean done = false;
        while (!done) {
            String line = dis.readLine();
            if (line == null) {
                // end of stream
                done = true;
            } else {
                IOR += line;
                if (IOR.startsWith("IOR:")) {
                    // the IOR does not span a line, so we're done
                    done = true;
                }
            }
        }
        dis.close();

        if (!IOR.startsWith("IOR:")) throw new IOException("Invalid IOR: " + IOR);

        return IOR;
    }
}