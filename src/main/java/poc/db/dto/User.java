package poc.db.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.bson.types.ObjectId;

@Getter
@Setter
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class User {

  private ObjectId _id;
  private String name;
  private String email;
  private String password;

}
