package com.michilla.dao.OHSAS;

import com.michilla.beans.OHSAS.OHSASMes;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.List;


public class OHSASMesDAO
{
    /*Obtiene un listado de meses en los que un programa tiene actividades para un centro costo determinado*/
    public static List<OHSASMes> listaMesProgramaSeleccion(String hostBD, 
                                                             String usuarioBD, 
                                                             String passwordBD,
                                                             String codigoEmpresa,
                                                             String codigoPrograma,
                                                             String codigoArea)
    {
        List<OHSASMes> listaMesPrograma = new ArrayList<OHSASMes>();
        try
        {
            DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
            Connection conn = 
                DriverManager.getConnection(hostBD, usuarioBD, passwordBD);
            
            String consulta = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
            
            PreparedStatement pstmt = conn.prepareStatement(consulta);
            pstmt.setString(1,codigoEmpresa);
            pstmt.setString(2,codigoPrograma);
            pstmt.setString(3,codigoArea);
            ResultSet resultado = pstmt.executeQuery();
            while (resultado.next())
            {
                OHSASMes m = new OHSASMes();
                m.setFecha(resultado.getString("FECHA"));
                m.setMes(resultado.getString("MES"));  
                m.setAnio(resultado.getString("ANIO"));
                listaMesPrograma.add(m);
            }
            pstmt.close();
            resultado.close();
            conn.close();
        }
        catch (SQLException e)
        {
            System.out.println("SMPR OHSAS: " +e.toString());
        }
        return listaMesPrograma;
    }
}
