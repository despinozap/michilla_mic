package com.michilla.dao.OHSAS;

import com.michilla.beans.CentroCosto;
import com.michilla.beans.OHSAS.OHSASMacro;
import com.michilla.beans.OHSAS.OHSASMes;
import com.michilla.beans.OHSAS.OHSASOperacion;
import com.michilla.beans.OHSAS.OHSASOperacionMensual;
import com.michilla.beans.OHSAS.OHSASPrograma;
import com.michilla.beans.Personal;
import com.michilla.genetico.AG;
import com.michilla.genetico.Individuo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.List;


public class OHSASOperacionResponsableDAO
{
    /*Crea un conjunto de relaciones entre responsable y actividad*/
    public static void create(String hostBD,String usuarioBD,String passwordBD,String codigoEmpresa,String idPrograma,
        String codigoCentroCosto,String fechaInicio,String fechaFin,List<OHSASMacro> listaMacro) 
        throws SQLException, NumberFormatException
    {
        DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
        Connection conn = DriverManager.getConnection(hostBD,usuarioBD,passwordBD);
        conn.setAutoCommit(false);
        try
        {
            /*Primero borramos todas las macro operaciones del programa*/
             /*Se borran las operaciones responsables*/

              String borrarOperacionResponsable = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
              
              PreparedStatement pstmt = conn.prepareStatement(borrarOperacionResponsable);
              pstmt.setString(1,codigoEmpresa);
              pstmt.setString(2,idPrograma);
              pstmt.setString(3,fechaInicio);
              pstmt.setString(4,fechaFin);
              pstmt.setString(5,codigoCentroCosto);
              pstmt.executeUpdate();
              pstmt.close();
             /*Se borran las operaciones*/

             String borrarOperaciones = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
             
             pstmt = conn.prepareStatement(borrarOperaciones);
             pstmt.setString(1,codigoEmpresa);
             pstmt.setString(2,idPrograma);
             pstmt.setString(3,fechaInicio);
             pstmt.setString(4,fechaFin);
             pstmt.setString(5,codigoCentroCosto);
             pstmt.executeUpdate();
             pstmt.close();

            String borrarMacro = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
            
            pstmt = conn.prepareStatement(borrarMacro);
            pstmt.setString(1,codigoEmpresa);
            pstmt.setString(2,idPrograma);
            pstmt.setString(3,fechaInicio);
            pstmt.setString(4,fechaFin);
            pstmt.setString(5,codigoCentroCosto);
            pstmt.executeUpdate();
            pstmt.close();
            for(OHSASMacro m : listaMacro)
            {
                /*Se insertan las macro operaciones*/

                String insertaMacro = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
                
                pstmt = conn.prepareStatement(insertaMacro);
                pstmt.setString(1,codigoEmpresa);
                pstmt.setString(2,m.getIdMacro());
                pstmt.setString(3,idPrograma);
                pstmt.setString(4,fechaInicio);
                pstmt.setString(5,fechaFin);
                pstmt.setString(6,codigoCentroCosto);
                pstmt.setString(7,m.getDescripcionMacro());
                pstmt.setString(8,"S");
                pstmt.executeUpdate();
                pstmt.close();
                /*Luego,por cada macro operacion, se insertan las operaciones*/
                for (OHSASOperacion o : m.getListaActividades())
                {
                    String insertaOperacion = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
                    
                    pstmt = conn.prepareStatement(insertaOperacion);
                    pstmt.setString(1,codigoEmpresa);
                    pstmt.setString(2,m.getIdMacro());
                    pstmt.setString(3,idPrograma);
                    pstmt.setString(4,fechaInicio);
                    pstmt.setString(5,fechaFin);
                    pstmt.setString(6,codigoCentroCosto);
                    pstmt.setString(7,o.getIdActividad());
                    pstmt.setString(8,o.getDescripcion());
                    pstmt.setString(9,"S");
                    pstmt.setInt(10,o.getPeriodo());
                    pstmt.setString(11,o.getNumeroPersona());
                    pstmt.executeUpdate();
                    pstmt.close();
                }
            }

            /************************************ PARTE DEL ALGORITMO GENETICO ********************************/
            List<Personal> listaPersonal = new ArrayList<Personal>();

            String seleccionarPersonalPorArea = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
             
             pstmt = conn.prepareStatement(seleccionarPersonalPorArea);
             pstmt.setString(1,codigoEmpresa);
             pstmt.setString(2,codigoCentroCosto);
             pstmt.setString(3,idPrograma);
             pstmt.setString(4,fechaInicio);
             pstmt.setString(5,fechaFin);
             ResultSet resultado = pstmt.executeQuery();
             while(resultado.next())
             {
                 Personal p = new Personal();
                 p.setRut(resultado.getString("RUT"));
                 listaPersonal.add(p);
                 
             }
            pstmt.close();
            resultado.close();
            Individuo mejor = AG.AlgoritmoGenetico(listaMacro,listaPersonal,fechaInicio,fechaFin);
            for(int i = 0; i < mejor.getListaPersonal().size(); i++)
            {
                for (int j = 0; j < mejor.getListaMes().size(); j++)
                {
                    String fecha = mejor.getListaMes().get(j).getFecha();
                    String rut = mejor.getListaPersonal().get(i).getRut();
                    if (mejor.getMatriz()[i][j] != null)
                    {
                        String[] operaciones = mejor.getMatriz()[i][j].split(";");
                        for (int k = 0; k < operaciones.length; k++)
                        {
                            String[] parseOperacion = operaciones[k].split("[a-z]*[0-9]*");
                            String macroParseado = parseOperacion[1];

                            String consulta = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
                            
                            pstmt = conn.prepareStatement(consulta);
                            pstmt.setString(1,codigoEmpresa);
                            pstmt.setString(2,fecha);
                            pstmt.setString(3,rut);
                            pstmt.setString(4,operaciones[k]);
                            pstmt.setString(5,macroParseado);
                            pstmt.setString(6,idPrograma);
                            pstmt.setString(7,fechaInicio);
                            pstmt.setString(8,fechaFin);
                            pstmt.setString(9,codigoCentroCosto);
                            pstmt.setString(10,"N");
                            pstmt.setString(11,"N");
                            pstmt.setString(12,"N");
                            pstmt.executeUpdate();
                            pstmt.close();
                        }  
                    }
                }
            }
            /**************************************************************************************************/
            conn.commit();
        }
        catch (SQLException e)
        {
            conn.rollback();
            System.out.println("SMPR/OHSAS/OHSASOperacionResponsableDAO/create: " +e.toString());
            throw e;
        }
        catch (NumberFormatException e)
        {
            conn.rollback();
            System.out.println("SMPR/OHSAS/OHSASOperacionResponsableDAO/create: " +e.toString());
            throw e;
        }
        finally
        {
            conn.close();
        }
        
    }
    public static void limpiarCalendario(String hostBD,String usuarioBD,String passwordBD,String codigoEmpresa,
        String idPrograma,String fechaInicio,String fechaFin,String codigoCentroCosto) throws SQLException
    {
        try
        {
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
            Connection conn = 
                DriverManager.getConnection(hostBD, usuarioBD, passwordBD);
            /*Se borran las operaciones responsables*/

             String borrarOperacionResponsable = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
             
             PreparedStatement pstmt = conn.prepareStatement(borrarOperacionResponsable);
             pstmt.setString(1,codigoEmpresa);
             pstmt.setString(2,idPrograma);
             pstmt.setString(3,fechaInicio);
             pstmt.setString(4,fechaFin);
             pstmt.setString(5,codigoCentroCosto);
             pstmt.executeUpdate();
             pstmt.close();
        }
        catch(SQLException e)
        {
            System.out.println("SMPR/OHSAS/OHSASOperacionResponsableDAO/limpiarCalendario: " +e.toString());
            throw e;
        }
    }
    
    /*Elimina una relacion entre una operacion y un empleado*/
    public static void delete(String hostBD,String usuarioBD,String passwordBD,String codigoEmpresa,String codigoMacro,
        String codigoOperacion,String codigoPrograma,String codigoCentroCosto,String rut,String fecha)
        throws SQLException
    {
        try
        {
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
            Connection conn = DriverManager.getConnection(hostBD,usuarioBD,passwordBD);
            
            String consulta = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
            
            PreparedStatement pstmt = conn.prepareStatement(consulta);
            pstmt.setString(1,codigoEmpresa);
            pstmt.setString(2,codigoMacro);
            pstmt.setString(3,codigoPrograma);
            pstmt.setString(4,codigoCentroCosto);
            pstmt.setString(5,codigoOperacion);
            pstmt.setString(6,rut);
            pstmt.setString(7,fecha);
            pstmt.executeUpdate();
            pstmt.close();
            conn.close();
        }
        catch (SQLException e)
        {
            System.out.println("SMPR/OHSAS/OHSASOperacionResponsableDAO/delete: " +e.toString());
            throw e;
        }
    }
    /*Actualiza la realacion entre una operacion y un empleado, marca si esta fue realizada*/
    public static void marcaRealizado(String hostBD,String usuarioBD,String passwordBD,String codigoEmpresa,
        String codigoPrograma,String fechaInicio,String fechaFin,String codigoMacro,String codigoOperacion,
        String codigoCentroCosto,String rut,String fecha,String realizado,String rutDigitador) throws SQLException
    {
        try
        {
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
            Connection conn = DriverManager.getConnection(hostBD,usuarioBD,passwordBD);
            
            String consulta = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
            
            PreparedStatement pstmt = conn.prepareStatement(consulta);
            pstmt.setString(1,realizado);
            pstmt.setString(2,rutDigitador);
            pstmt.setString(3,codigoEmpresa);
            pstmt.setString(4,codigoMacro);
            pstmt.setString(5,codigoPrograma);
            pstmt.setString(6,fechaInicio);
            pstmt.setString(7,fechaFin);
            pstmt.setString(8,codigoCentroCosto);
            pstmt.setString(9,codigoOperacion);
            pstmt.setString(10,rut);
            pstmt.setString(11,fecha);
            pstmt.executeUpdate();
            pstmt.close();
            conn.close();
        }
        catch (SQLException e)
        {
            System.out.println("SMPR/OHSAS/OHSASOperacionResponsableDAO/marcaRealizado: " +e.toString());
            throw e;
        }
    }
    /*Actualiza la realacion entre una operacion y un empleado, marca si esta fue realizada*/
    public static void evaluar(String hostBD,String usuarioBD,String passwordBD,String codigoEmpresa,String idPrograma,
        String codigoCentroCosto,String fechaInicio,String fechaFin,String rut,String fecha,String idOperacion,
        String idMacro,String revisado,String resultado,String observacion) throws SQLException
    {
        try
        {
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
            Connection conn = DriverManager.getConnection(hostBD,usuarioBD,passwordBD);
            
            String consulta = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
            
            PreparedStatement pstmt = conn.prepareStatement(consulta);
            pstmt.setString(1,revisado);
            pstmt.setString(2,resultado);
            pstmt.setString(3,observacion);
            pstmt.setString(4,codigoEmpresa);
            pstmt.setString(5,fecha);
            pstmt.setString(6,rut);
            pstmt.setString(7,idOperacion);
            pstmt.setString(8,idMacro);
            pstmt.setString(9,idPrograma);
            pstmt.setString(10,fechaInicio);
            pstmt.setString(11,fechaFin);
            pstmt.setString(12,codigoCentroCosto);
            pstmt.executeUpdate();
            pstmt.close();
            conn.close();
        }
        catch (SQLException e)
        {
            System.out.println("SMPR/OHSAS/OHSASOperacionResponsableDAO/evaluar: " +e.toString());
            throw e;
        }
    }
    /*Actualiza la realacion entre una operacion y un empleado, marca si esta fue revisada*/
    public static void marcaRevisado(String hostBD,String usuarioBD,String passwordBD,String codigoEmpresa,
        String codigoPrograma,String codigoMacro,String codigoOperacion,String codigoCentroCosto,String rut,
        String fecha,String revisado) throws SQLException
    {
        try
        {
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
            Connection conn = DriverManager.getConnection(hostBD,usuarioBD,passwordBD);
            
            String consulta = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
            
            PreparedStatement pstmt = conn.prepareStatement(consulta);
            pstmt.setString(1,revisado);
            pstmt.setString(2,codigoEmpresa);
            pstmt.setString(3,codigoMacro);
            pstmt.setString(4,codigoPrograma);
            pstmt.setString(5,codigoCentroCosto);
            pstmt.setString(6,codigoOperacion);
            pstmt.setString(7,rut);
            pstmt.setString(8,fecha);
            pstmt.executeUpdate();
            pstmt.close();
            conn.close();
        }
        catch (SQLException e)
        {
            System.out.println("SMPR/OHSAS/OHSASOperacionResponsableDAO/marcaRevisado: " +e.toString());
            throw e;
        }
    }
    /*Obtiene un listado de personas de una area con sus respectivas operaciones*/
    public static List<Personal> listaPersonalActividad(String hostBD,String usuarioBD,String passwordBD,
        String codigoEmpresa,String centroCosto,String idPrograma,String fechaInicio,String fechaFin)
    {
        List<Personal> listaTrabajadores = new ArrayList<Personal>();
        try
        {
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
            Connection conn = DriverManager.getConnection(hostBD,usuarioBD,passwordBD);
            
            String consulta = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
            
            PreparedStatement pstmt = conn.prepareStatement(consulta);
            pstmt.setString(1,codigoEmpresa);
            pstmt.setString(2,idPrograma);
            pstmt.setString(3,fechaInicio);
            pstmt.setString(4,fechaFin);
            pstmt.setString(5,centroCosto);
            ResultSet resultado = pstmt.executeQuery();
            while (resultado.next())
            {
                Personal p = new Personal();
                p.setRut(resultado.getString("RUT"));
                p.setNumeroOHSAS(resultado.getInt("NUMERO"));
                p.setNombres(resultado.getString("NOMBRES"));
                p.setApellidoPaterno(resultado.getString("APELLIDO_PATERNO"));
                p.setApellidoMaterno(resultado.getString("APELLIDO_MATERNO"));
                p.setEmail(resultado.getString("EMAIL"));
                Personal revisor = new Personal();
                revisor.setRut(resultado.getString("RUT_REVISOR"));
                revisor.setNombres(resultado.getString("NOMBRES_REVISOR"));
                revisor.setApellidoPaterno(resultado.getString("APELLIDO_PATERNO_REVISOR"));
                revisor.setApellidoMaterno(resultado.getString("APELLIDO_MATERNO_REVISOR"));
                p.setPersonalRevisorOHSAS(revisor);
                listaTrabajadores.add(p);
            }
            pstmt.close();
            resultado.close();
            for (Personal p : listaTrabajadores)
            {
                List<OHSASOperacionMensual> listaActividadMensual = new ArrayList<OHSASOperacionMensual>();
                
                String obtieneActividades = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
                
                pstmt = conn.prepareStatement(obtieneActividades);
                pstmt.setString(1,codigoEmpresa);
                pstmt.setString(2,idPrograma);
                pstmt.setString(3,centroCosto);
                pstmt.setString(4,p.getRut());
                pstmt.setString(5,fechaInicio);
                pstmt.setString(6,fechaFin);
                resultado = pstmt.executeQuery();
                while (resultado.next())
                {
                    OHSASOperacionMensual oMes = new OHSASOperacionMensual();
                    oMes.setObservacion(resultado.getString("OBSERVACIONES"));
                    oMes.setRealizado(resultado.getString("REALIZADO"));
                    oMes.setResultado(resultado.getString("RESULTADO"));
                    oMes.setRevisado(resultado.getString("REVISADO"));
                    OHSASMes m = new OHSASMes();
                    m.setFecha(resultado.getString("FECHA"));
                    oMes.setMes(m);
                    OHSASMes fr = new OHSASMes();
                    fr.setFecha(resultado.getString("FECHA_REALIZADO"));
                    oMes.setFechaRealizado(fr);
                    OHSASOperacion ac = new OHSASOperacion();
                    ac.setIdActividad(resultado.getString("ID_OPERACION"));
                    oMes.setActividad(ac);
                    OHSASMacro om = new OHSASMacro();
                    om.setIdMacro(resultado.getString("ID_MACRO_OPER"));
                    oMes.setMacro(om);
                    listaActividadMensual.add(oMes);
                }
                pstmt.close();
                resultado.close();
                p.setListaActividadMensual(listaActividadMensual);
            }
            conn.close();
        }
        catch (SQLException e)
        {
            System.out.println("SMPR/OHSAS/OHSASOperacionResponsableDAO/listaPersonalActividad: " + e.toString());
        }
        return listaTrabajadores;
    }
    
    /*Obtiene un listado de personas de una area con sus respectivas operaciones*/
    public static Personal getPersonalActividad(String hostBD,String usuarioBD,String passwordBD,
        String codigoEmpresa,String centroCosto,String idPrograma,String fechaInicio,String fechaFin,String rut)
    {
        Personal p = new Personal();
        try
        {
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
            Connection conn = DriverManager.getConnection(hostBD,usuarioBD,passwordBD);
            
            String consulta = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
            
            PreparedStatement pstmt = conn.prepareStatement(consulta);
            pstmt.setString(1,codigoEmpresa);
            pstmt.setString(2,idPrograma);
            pstmt.setString(3,fechaInicio);
            pstmt.setString(4,fechaFin);
            pstmt.setString(5,centroCosto);
            pstmt.setString(6,rut);
            ResultSet resultado = pstmt.executeQuery();
            while (resultado.next())
            {
                p.setRut(resultado.getString("RUT"));
                p.setNumeroOHSAS(resultado.getInt("NUMERO"));
                p.setNombres(resultado.getString("NOMBRES"));
                p.setApellidoPaterno(resultado.getString("APELLIDO_PATERNO"));
                p.setApellidoMaterno(resultado.getString("APELLIDO_MATERNO"));
                p.setEmail(resultado.getString("EMAIL"));
                Personal revisor = new Personal();
                revisor.setRut(resultado.getString("RUT_REVISOR"));
                revisor.setNombres(resultado.getString("NOMBRES_REVISOR"));
                revisor.setApellidoPaterno(resultado.getString("APELLIDO_PATERNO_REVISOR"));
                revisor.setApellidoMaterno(resultado.getString("APELLIDO_MATERNO_REVISOR"));
                p.setPersonalRevisorOHSAS(revisor);
            }
            pstmt.close();
            resultado.close();
            List<OHSASOperacionMensual> listaActividadMensual = new ArrayList<OHSASOperacionMensual>();
            
            String obtieneActividades = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
            
            pstmt = conn.prepareStatement(obtieneActividades);
            pstmt.setString(1,codigoEmpresa);
            pstmt.setString(2,idPrograma);
            pstmt.setString(3,centroCosto);
            pstmt.setString(4,rut);
            pstmt.setString(5,fechaInicio);
            pstmt.setString(6,fechaFin);
            resultado = pstmt.executeQuery();
            while (resultado.next())
            {
                OHSASOperacionMensual oMes = new OHSASOperacionMensual();
                oMes.setObservacion(resultado.getString("OBSERVACIONES"));
                oMes.setRealizado(resultado.getString("REALIZADO"));
                oMes.setResultado(resultado.getString("RESULTADO"));
                oMes.setRevisado(resultado.getString("REVISADO"));
                OHSASMes m = new OHSASMes();
                m.setFecha(resultado.getString("FECHA"));
                oMes.setMes(m);
                OHSASMes fr = new OHSASMes();
                fr.setFecha(resultado.getString("FECHA_REALIZADO"));
                oMes.setFechaRealizado(fr);
                OHSASOperacion ac = new OHSASOperacion();
                ac.setIdActividad(resultado.getString("ID_OPERACION"));
                oMes.setActividad(ac);
                OHSASMacro om = new OHSASMacro();
                om.setIdMacro(resultado.getString("ID_MACRO_OPER"));
                oMes.setMacro(om);
                listaActividadMensual.add(oMes);
            }
            pstmt.close();
            resultado.close();
            p.setListaActividadMensual(listaActividadMensual);
            conn.close();
        }
        catch (SQLException e)
        {
            System.out.println("SMPR/OHSAS/OHSASOperacionResponsableDAO/getPersonalActividad: " + e.toString());
        }
        return p;
    }
    /*Obtiene las operaciones de una persona*/
    public static List<OHSASOperacionMensual> getPersonalActividad(String hostBD,String usuarioBD,String passwordBD,
        String codigoEmpresa,String rut)
    {
        List<OHSASOperacionMensual> listaOperacionMensual = new ArrayList<OHSASOperacionMensual>();
        try
        {
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
            Connection conn = DriverManager.getConnection(hostBD,usuarioBD,passwordBD);
            /*Se obtienen los programas en los que participa la persona*/
            
            String consulta = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
            
            PreparedStatement pstmt = conn.prepareStatement(consulta);
            pstmt.setString(1,codigoEmpresa);
            pstmt.setString(2,rut);
            ResultSet resultado = pstmt.executeQuery();
            while(resultado.next())
            {
                OHSASOperacionMensual om = new OHSASOperacionMensual();
                OHSASMes m = new OHSASMes();
                m.setFecha(resultado.getString("FECHA"));
                m.setAnio(resultado.getString("ANIO"));
                m.setMes(resultado.getString("MES"));
                om.setMes(m);
                om.setObservacion(resultado.getString("OBSERVACIONES"));
                om.setRealizado(resultado.getString("REALIZADO"));
                om.setResultado(resultado.getString("RESULTADO"));
                om.setRevisado(resultado.getString("REVISADO"));
                OHSASOperacion o = new OHSASOperacion();
                o.setIdActividad(resultado.getString("ID_OPERACION"));
                o.setDescripcion(resultado.getString("DESCRIPCION_OPERACION"));
                om.setActividad(o);
                OHSASPrograma p = new OHSASPrograma();
                p.setIdPrograma(resultado.getString("ID_PROGRAMA"));
                p.setDescripcion(resultado.getString("DESCRIPCION_PROGRAMA"));
                om.setPrograma(p);
                OHSASMacro mc = new OHSASMacro();
                mc.setIdMacro(resultado.getString("ID_MACRO_OPER"));
                mc.setDescripcionMacro(resultado.getString("DESCRIPCION_MACRO"));
                om.setMacro(mc);
                listaOperacionMensual.add(om);
            }
            pstmt.close();
            resultado.close();
            conn.close();
        }
        catch (SQLException e)
        {
            System.out.println("SMPR/OHSAS/OHSASOperacionResponsableDAO/getPersonalActividad: " + e.toString());
        }
        return listaOperacionMensual;
    }
    /*Cuenta las actividades que se efectuan en un mes, un centro costo y programa determinado determinado*/
    public static int cuentaActividades(String hostBD,String usuarioBD,String passwordBD,String codigoEmpresa,
        String codigoCentroCosto,String idPrograma,String fecha)
    {
        int cuenta = 0; 
        try
        {
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
            Connection conn = DriverManager.getConnection(hostBD, usuarioBD, passwordBD);
            
            String consultaActividadMensual = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
            
            PreparedStatement pstmt = conn.prepareStatement(consultaActividadMensual);
            pstmt.setString(1,codigoEmpresa);
            pstmt.setString(2,idPrograma);
            pstmt.setString(3,codigoCentroCosto);
            pstmt.setString(4,fecha);
            ResultSet resultado = pstmt.executeQuery();
            while (resultado.next())
            {
                cuenta = resultado.getInt("CUENTA");
            }
            resultado.close();
            pstmt.close();
            conn.close();
        }
        catch (SQLException e)
        {
            System.out.println("SMPR/OHSAS/OHSASOperacionResponsableDAO/cuentaActividades: " +e.toString());
        }
        return cuenta;
    }
    /*Entrega un programa con una actividad buscada*/
    public static OHSASPrograma getOperacion(String hostBD,String usuarioBD,String passwordBD,String codigoEmpresa,
        String codigoCentroCosto,String idPrograma,String fecha,String fechaInicio,String fechaFin,String idOperacion,
        String idMacro,String rut)
    {
        OHSASPrograma programa = new OHSASPrograma(); 
        try
        {
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
            Connection conn = DriverManager.getConnection(hostBD,usuarioBD,passwordBD);
            
            String consultaActividadMensual = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
            
            PreparedStatement pstmt = conn.prepareStatement(consultaActividadMensual);
            pstmt.setString(1,codigoEmpresa);
            pstmt.setString(2,fecha);
            pstmt.setString(3,rut);
            pstmt.setString(4,idOperacion);
            pstmt.setString(5,idMacro);
            pstmt.setString(6,idPrograma);
            pstmt.setString(7,fechaInicio);
            pstmt.setString(8,fechaFin);
            pstmt.setString(9,codigoCentroCosto);
            ResultSet resultado = pstmt.executeQuery();
            while (resultado.next())
            {
                programa.setIdPrograma(resultado.getString("ID_PROGRAMA"));
                programa.setDescripcion(resultado.getString("DESCRIPCION_PROGRAMA"));
                CentroCosto cc = new CentroCosto();
                cc.setCodigo(resultado.getInt("CODIGO_CENTRO_COSTO"));
                cc.setDescripcion(resultado.getString("DESCRIPCION_COSTO"));
                programa.setCentroCosto(cc);
                OHSASMes fi = new OHSASMes();
                fi.setFecha(resultado.getString("FECHA_INICIO"));
                programa.setFechaInicio(fi);
                OHSASMes ff = new OHSASMes();
                ff.setFecha(resultado.getString("FECHA_FIN"));
                programa.setFechaFin(ff);
                List<OHSASMacro> lm = new ArrayList<OHSASMacro>();
                OHSASMacro m = new OHSASMacro();
                m.setIdMacro(resultado.getString("ID_MACRO_OPER"));
                m.setDescripcionMacro(resultado.getString("DESCRIPCION_MACRO"));
                List<OHSASOperacion> lo = new ArrayList<OHSASOperacion>();
                OHSASOperacion o = new OHSASOperacion();
                o.setIdActividad(resultado.getString("ID_OPERACION"));
                o.setDescripcion(resultado.getString("DESCRIPCION_OPERACION"));
                List<OHSASOperacionMensual> lom = new ArrayList<OHSASOperacionMensual>();
                OHSASOperacionMensual om = new OHSASOperacionMensual();
                om.setRealizado(resultado.getString("REALIZADO"));
                om.setRevisado(resultado.getString("REVISADO"));
                om.setResultado(resultado.getString("RESULTADO"));
                om.setObservacion(resultado.getString("OBSERVACIONES"));
                om.setVencido(resultado.getString("VENCIDO"));
                Personal p = new Personal();
                p.setNombres(resultado.getString("NOMBRE"));
                p.setEmail(resultado.getString("EMAIL"));
                om.setPersonal(p);
                OHSASMes mes = new OHSASMes();
                mes.setFecha(resultado.getString("FECHA"));
                om.setMes(mes);
                lom.add(om);
                o.setListaActividadMensual(lom);
                lo.add(o);
                m.setListaActividades(lo);
                lm.add(m);
                programa.setListaMacro(lm);
            }
            resultado.close();
            pstmt.close();
            conn.close();
        }
        catch (SQLException e)
        {
            System.out.println("SMPR/OHSAS/OHSASOperacionResponsableDAO/getOperacion: " +e.toString());
        }
        return programa;
    }
    /*Entrega un programa con una actividad buscada*/
    public static Personal getPersonalPorNumero(String hostBD,String usuarioBD,String passwordBD,String codigoEmpresa,
        String codigoCentroCosto,String idPrograma,String fechaInicio,String fechaFin,String numero)
    {
        Personal personal = new Personal(); 
        try
        {
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
            Connection conn = DriverManager.getConnection(hostBD,usuarioBD,passwordBD);
            
            String consultaPersonal = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
            
            PreparedStatement pstmt = conn.prepareStatement(consultaPersonal);
            pstmt.setString(1,codigoEmpresa);
            pstmt.setString(2,idPrograma);
            pstmt.setString(3,fechaInicio);
            pstmt.setString(4,fechaFin);
            pstmt.setString(5,codigoCentroCosto);
            pstmt.setString(6,numero);
            ResultSet resultado = pstmt.executeQuery();
            while (resultado.next())
            {
                personal.setRut(resultado.getString("RUT"));
                personal.setNombres(resultado.getString("NOMBRES"));
                personal.setApellidoPaterno(resultado.getString("APELLIDO_PATERNO"));
                personal.setApellidoMaterno(resultado.getString("APELLIDO_MATERNO"));
                personal.setEmail(resultado.getString("EMAIL"));
            }
            resultado.close();
            pstmt.close();
            conn.close();
        }
        catch (SQLException e)
        {
            System.out.println("SMPR/OHSAS/OHSASOperacionResponsableDAO/getPersonalPorNumero: " +e.toString());
        }
        return personal;
    }
    /*Actualiza las observaciones de una actividad*/
    public static void actualizaObservacion(String hostBD,String usuarioBD,String passwordBD,String codigoEmpresa,
        String codigoPrograma,String codigoMacro,String codigoOperacion,String codigoCentroCosto,String rut,
        String fecha,String observacion) throws SQLException
    {
        try
        {
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
            Connection conn = DriverManager.getConnection(hostBD, usuarioBD, passwordBD);
            
            String consulta = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */ 
            
            PreparedStatement pstmt = conn.prepareStatement(consulta);
            pstmt.setString(1,observacion);
            pstmt.setString(2,codigoEmpresa);
            pstmt.setString(3,codigoMacro);
            pstmt.setString(4,codigoPrograma);
            pstmt.setString(5,codigoCentroCosto);
            pstmt.setString(6,codigoOperacion);
            pstmt.setString(7,rut);
            pstmt.setString(8,fecha);
            pstmt.executeUpdate();
            pstmt.close();
            conn.close();
        }
        catch (SQLException e)
        {
            System.out.println("SMPR/OHSAS/OHSASOperacionResponsableDAO/actualizaObservacion: " +e.toString());
            throw e;
        }
    }
}
