package server;


import javax.servlet.http.*;
import javax.servlet.annotation.*;
import java.io.IOException;

/**
 * @author yuezhao
 */
@WebServlet(name = "SkierServlet")
public class SkierServlet extends HttpServlet {

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
    res.setContentType("text/plain");
    String urlPath = req.getPathInfo();

    if (urlPath == null || urlPath.isEmpty()) {
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
      res.getWriter().write("missing parameters");
      return;
    }

    String[] urlParts = urlPath.split("/");

    if (!isUrlValid(urlParts)) {
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
      res.getWriter().write("invalid parameters");
    } else {
      res.setStatus(HttpServletResponse.SC_OK);
      // TODO: process url params in `urlParts`
      res.getWriter().write("It works!");
    }
  }


  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException {
    res.setContentType("application/json");
    String urlPath = req.getPathInfo();

    if (urlPath == null || urlPath.isEmpty()) {
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
      res.getWriter().write("missing parameters");
      return;
    }

    String[] urlParts = urlPath.split("/");

    if (!isUrlValid(urlParts)) {
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
      res.getWriter().write("invalid parameters");
    } else {
      res.setStatus(HttpServletResponse.SC_OK);
      // TODO: process url params in `urlParts`
      res.getWriter().write("It works!");
    }
  }


  private boolean isUrlValid(String[] urlParts) {
    if (urlParts.length == 8) {
      try {
        Integer.parseInt(urlParts[1]);
        Integer.parseInt(urlParts[3]);
        Integer.parseInt(urlParts[5]);
        Integer.parseInt(urlParts[7]);
        return urlParts[2].equals("seasons") &&
                urlParts[4].equals("days") &&
                urlParts[6].equals("skiers");
      } catch (NumberFormatException e) {
        return false;
      }
    }
    return false;
  }
}