package com.michilla.dao.OHSAS;

import com.michilla.beans.Personal;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import java.util.ArrayList;
import java.util.List;


public class OHSASNumeroPersonalDAO
{
    public OHSASNumeroPersonalDAO()
    {
    }
    /*Obtiene un listado de personas de un area con su numero para un programa del programa OHSAS*/
     public static List<Personal> listaPersonalNumero(String hostBD, 
                                                     String usuarioBD, 
                                                     String passwordBD,
                                                     String codigoEmpresa,
                                                     String idPrograma,
                                                     String fechaInicio,
                                                     String fechaFin,
                                                     String codigoCentroCosto
                                                    )
     {
         List<Personal> listaPersonal = new ArrayList<Personal>();
         try
         {
             DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
             Connection conn = 
                 DriverManager.getConnection(hostBD, usuarioBD, passwordBD);
             
             String consulta = /* THIS SQL QUERY HAS BEEN HIDDEN FOR CONFIDENTIALITY PURPOSES */
             
             PreparedStatement pstmt = conn.prepareStatement(consulta);
             pstmt.setString(1,codigoEmpresa);
             pstmt.setString(2,idPrograma);
             pstmt.setString(3,fechaInicio);
             pstmt.setString(4,fechaFin);
             pstmt.setString(5,codigoCentroCosto);
             ResultSet resultado = pstmt.executeQuery();
             while (resultado.next())
             {
                 Personal p = new Personal();
                 p.setRut(resultado.getString("RUT"));
                 p.setNumeroOHSAS(resultado.getInt("NUMERO"));
                 p.setNombres(resultado.getString("NOMBRES"));
                 p.setApellidoPaterno(resultado.getString("APELLIDO_PATERNO"));
                 p.setApellidoMaterno(resultado.getString("APELLIDO_MATERNO"));
                 listaPersonal.add(p);
             }
             pstmt.close();
             resultado.close();
             conn.close();
         }
         catch (SQLException e)
         {
             System.out.println("SMPR OHSAS: " +e.toString());
         }
         return listaPersonal;
     }
}
