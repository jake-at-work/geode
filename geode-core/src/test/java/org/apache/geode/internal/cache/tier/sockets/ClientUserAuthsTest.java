/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package org.apache.geode.internal.cache.tier.sockets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.STRICT_STUBS;

import java.util.OptionalLong;

import org.apache.shiro.subject.Subject;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoSettings;

@MockitoSettings(strictness = STRICT_STUBS)
class ClientUserAuthsTest {
  @Nested
  class PutUserAuth {
    @Mock
    SubjectIdGenerator idGenerator;

    @Test
    void associatesUserAuthWithGeneratedId() {
      var auths = new ClientUserAuths(idGenerator);

      var idFromGenerator = -234950983L;
      when(idGenerator.generateId())
          .thenReturn(OptionalLong.of(idFromGenerator));

      var userAuth = mock(UserAuthAttributes.class);

      long returnedId = auths.putUserAuth(userAuth);

      var userAuthWithGeneratedId = auths.getUserAuthAttributes(idFromGenerator);
      assertThat(userAuthWithGeneratedId)
          .as("user auth with generated ID")
          .isSameAs(userAuth);

      assertThat(returnedId)
          .as("returned ID")
          .isEqualTo(idFromGenerator);
    }

    @Test
    void associatesUserAuthWithFirstAssignableGeneratedIdIfInitialIdsNotAssignable() {
      var auths = new ClientUserAuths(idGenerator);

      var nextAssignableIdFromGenerator = -9341L;
      when(idGenerator.generateId())
          .thenReturn(OptionalLong.of(0L)) // 0 requests a generated ID, so we must not assign it.
          .thenReturn(OptionalLong.of(0L))
          .thenReturn(OptionalLong.of(-1L)) // -1 requests a generated ID, so we must not assign it.
          .thenReturn(OptionalLong.of(-1L))
          .thenReturn(OptionalLong.of(0L))
          .thenReturn(OptionalLong.of(nextAssignableIdFromGenerator));

      var userAuth = mock(UserAuthAttributes.class);

      long returnedId = auths.putUserAuth(userAuth);

      var userAuthWithGeneratedId =
          auths.getUserAuthAttributes(nextAssignableIdFromGenerator);
      assertThat(userAuthWithGeneratedId)
          .as("user auth with generated ID")
          .isSameAs(userAuth);

      assertThat(returnedId)
          .as("returned ID")
          .isEqualTo(nextAssignableIdFromGenerator);
    }

    @Test
    void associatesUserAuthWithFirstAssignableGeneratedIdIfGeneratorExhausted() {
      var auths = new ClientUserAuths(idGenerator);

      // The generator returns an empty optional to indicate that it has run out of unique IDs,
      // and that subsequent IDs will include previously-generated values.
      var uniqueIdsExhausted = OptionalLong.empty();
      var nextAssignableIdFromGenerator = -9341L;
      when(idGenerator.generateId())
          .thenReturn(uniqueIdsExhausted)
          .thenReturn(OptionalLong.of(0)) // Non-assignable
          .thenReturn(OptionalLong.of(-1)) // Non-assignable
          .thenReturn(OptionalLong.of(-1)) // Non-assignable
          .thenReturn(OptionalLong.of(0)) // Non-assignable
          .thenReturn(OptionalLong.of(nextAssignableIdFromGenerator));

      var userAuth = mock(UserAuthAttributes.class);

      long returnedId = auths.putUserAuth(userAuth);

      var userAuthWithGeneratedId =
          auths.getUserAuthAttributes(nextAssignableIdFromGenerator);
      assertThat(userAuthWithGeneratedId)
          .as("user auth with generated ID")
          .isSameAs(userAuth);

      assertThat(returnedId)
          .as("returned ID")
          .isEqualTo(nextAssignableIdFromGenerator);
    }

    @Test
    void removesExistingUserAuthsIfGeneratorExhausted() {
      var auths = new ClientUserAuths(idGenerator);

      // The generator returns an empty optional to indicate that it has run out of unique IDs,
      // and that subsequent IDs will include previously-generated values.
      var uniqueIdsExhausted = OptionalLong.empty();
      var userAuthId1 = 34L;
      var userAuthId2 = -563L;
      var userAuthId3 = 777L;
      var firstPostExhaustionIdFromGenerator = -9341L;
      when(idGenerator.generateId())
          .thenReturn(OptionalLong.of(userAuthId1))
          .thenReturn(OptionalLong.of(userAuthId2))
          .thenReturn(OptionalLong.of(userAuthId3))
          .thenReturn(uniqueIdsExhausted)
          .thenReturn(OptionalLong.of(firstPostExhaustionIdFromGenerator));

      // Given some existing user auths
      var existingUserAuth1 = mock(UserAuthAttributes.class, "existing auth 1");
      auths.putUserAuth(existingUserAuth1); // Gets userAuthId1
      var existingUserAuth2 = mock(UserAuthAttributes.class, "existing auth 2");
      auths.putUserAuth(existingUserAuth2); // Gets userAuthId2
      var existingUserAuth3 = mock(UserAuthAttributes.class, "existing auth 3");
      auths.putUserAuth(existingUserAuth3); // Gets userAuthId3

      var newAuth = mock(UserAuthAttributes.class);

      long returnedId = auths.putUserAuth(newAuth);

      assertThat(auths.getUserAuthAttributes(userAuthId1))
          .as("user auth with ID %d", userAuthId1)
          .isNull();
      assertThat(auths.getUserAuthAttributes(userAuthId2))
          .as("user auth with ID %d", userAuthId2)
          .isNull();
      assertThat(auths.getUserAuthAttributes(userAuthId3))
          .as("user auth with ID %d", userAuthId3)
          .isNull();

      var userAuthWithGeneratedId =
          auths.getUserAuthAttributes(firstPostExhaustionIdFromGenerator);
      assertThat(userAuthWithGeneratedId)
          .as("user auth with generated ID")
          .isSameAs(newAuth);

      assertThat(returnedId)
          .as("returned ID")
          .isEqualTo(firstPostExhaustionIdFromGenerator);
    }
  }

  // IDs 0 and -1 are special, non-assignable values that ask putSubject() to generate a unique ID
  // and associate the subject with it.
  @Nested
  class PutSubjectRequestingGeneratedId {
    @Mock
    SubjectIdGenerator idGenerator;

    @ParameterizedTest(name = "{displayName} givenId={0}")
    @ValueSource(longs = {0L, -1L})
    void associatesSubjectWithGeneratedId(long givenId) {
      var auths = new ClientUserAuths(idGenerator);

      var idFromGenerator = -234950983L;
      when(idGenerator.generateId())
          .thenReturn(OptionalLong.of(idFromGenerator));

      var subject = mock(Subject.class);

      var returnedId = auths.putSubject(subject, givenId);

      var subjectWithGeneratedId = auths.getSubject(idFromGenerator);
      assertThat(subjectWithGeneratedId)
          .as("subject with generated ID")
          .isSameAs(subject);

      assertThat(returnedId)
          .as("returned ID")
          .isEqualTo(idFromGenerator);

      var subjectWithGivenId = auths.getSubject(givenId);
      assertThat(subjectWithGivenId)
          .as("subject with given ID")
          .isNull();

      verifyNoInteractions(subject);
    }

    @ParameterizedTest(name = "{displayName} givenId={0}")
    @ValueSource(longs = {0L, -1L})
    void associatesSubjectWithFirstAssignableGeneratedIdIfInitialIdsNotAssignable(long givenId) {
      var auths = new ClientUserAuths(idGenerator);

      var nextAssignableIdFromGenerator = -9341L;
      when(idGenerator.generateId())
          .thenReturn(OptionalLong.of(0L)) // 0 requests a generated ID, so we must not assign it.
          .thenReturn(OptionalLong.of(0L))
          .thenReturn(OptionalLong.of(-1L)) // -1 requests a generated ID, so we must not assign it.
          .thenReturn(OptionalLong.of(-1L))
          .thenReturn(OptionalLong.of(0L))
          .thenReturn(OptionalLong.of(nextAssignableIdFromGenerator));

      var subject = mock(Subject.class);

      var returnedId = auths.putSubject(subject, givenId);

      var subjectWithSecondGeneratedId = auths.getSubject(nextAssignableIdFromGenerator);
      assertThat(subjectWithSecondGeneratedId)
          .as("subject with second generated ID")
          .isSameAs(subject);

      assertThat(returnedId)
          .as("returned ID")
          .isEqualTo(nextAssignableIdFromGenerator);

      var subjectWithGivenId = auths.getSubject(givenId);
      assertThat(subjectWithGivenId)
          .as("subject with given ID")
          .isNull();

      verifyNoInteractions(subject);
    }

    @ParameterizedTest(name = "{displayName} givenId={0}")
    @ValueSource(longs = {0L, -1L})
    void associatesSubjectWithFirstAssignableGeneratedIdIfGeneratorExhausted(long givenId) {
      var auths = new ClientUserAuths(idGenerator);

      // The generator returns an empty optional to indicate that it has run out of unique IDs,
      // and that subsequent IDs will include previously-generated values.
      var uniqueIdsExhausted = OptionalLong.empty();
      var nextAssignableIdFromGenerator = -9341L;
      when(idGenerator.generateId())
          .thenReturn(uniqueIdsExhausted)
          .thenReturn(OptionalLong.of(0)) // Non-assignable
          .thenReturn(OptionalLong.of(-1)) // Non-assignable
          .thenReturn(OptionalLong.of(-1)) // Non-assignable
          .thenReturn(OptionalLong.of(0)) // Non-assignable
          .thenReturn(OptionalLong.of(nextAssignableIdFromGenerator));

      var subject = mock(Subject.class);
      var returnedId = auths.putSubject(subject, givenId);

      var subjectWithGeneratedId = auths.getSubject(nextAssignableIdFromGenerator);
      assertThat(subjectWithGeneratedId)
          .as("subject with generated ID")
          .isSameAs(subject);

      assertThat(returnedId)
          .as("returned ID")
          .isEqualTo(nextAssignableIdFromGenerator);

      var subjectWithGivenId = auths.getSubject(givenId);
      assertThat(subjectWithGivenId)
          .as("subject with given ID")
          .isNull();

      verifyNoInteractions(subject);
    }

    @ParameterizedTest(name = "{displayName} givenId={0}")
    @ValueSource(longs = {0L, -1L})
    void removesExistingUserAuthorizationsIfGeneratorExhausted(long givenId) {
      var auths = new ClientUserAuths(idGenerator);

      // The generator returns an empty optional to indicate that it has run out of unique IDs,
      // and that subsequent IDs will include previously-generated values.
      var uniqueIdsExhausted = OptionalLong.empty();
      var userAuthId1 = 34L;
      var userAuthId2 = -563L;
      var userAuthId3 = 777L;
      var firstPostExhaustionIdFromGenerator = -9341L;
      when(idGenerator.generateId())
          .thenReturn(OptionalLong.of(userAuthId1))
          .thenReturn(OptionalLong.of(userAuthId2))
          .thenReturn(OptionalLong.of(userAuthId3))
          .thenReturn(uniqueIdsExhausted)
          .thenReturn(OptionalLong.of(firstPostExhaustionIdFromGenerator));

      // Given some existing user auths
      var userAuth1 = mock(UserAuthAttributes.class, "user auth 1");
      auths.putUserAuth(userAuth1); // Gets userAuthId1
      var userAuth2 = mock(UserAuthAttributes.class, "user auth 2");
      auths.putUserAuth(userAuth2); // Gets userAuthId2
      var userAuth3 = mock(UserAuthAttributes.class, "user auth 3");
      auths.putUserAuth(userAuth3); // Gets userAuthId3

      auths.putSubject(mock(Subject.class), givenId);

      assertThat(auths.getUserAuthAttributes(userAuthId1))
          .as("user auth with ID %d", userAuthId1)
          .isNull();
      assertThat(auths.getUserAuthAttributes(userAuthId2))
          .as("user auth with ID %d", userAuthId2)
          .isNull();
      assertThat(auths.getUserAuthAttributes(userAuthId3))
          .as("user auth with ID %d", userAuthId3)
          .isNull();
    }
  }

  @Nested
  class PutSubjectWithAssignableId {
    @Test
    void associatesSubjectWithGivenId() {
      var auths = new ClientUserAuths(null);

      // Neither 0 nor -1, and so gives the ID to associate with this subject
      var givenId = 3L;
      var subject = mock(Subject.class);

      var returnedId = auths.putSubject(subject, givenId);

      var subjectWithGivenId = auths.getSubject(givenId);
      assertThat(subjectWithGivenId)
          .as("subject with given ID")
          .isSameAs(subject);

      assertThat(returnedId)
          .as("returned ID")
          .isEqualTo(givenId);
    }

    @Test
    void doesNotInteractWithAddedSubject() {
      var auths = new ClientUserAuths(null);

      var subject = mock(Subject.class);

      auths.putSubject(subject, 674928374L);

      verifyNoInteractions(subject);
    }

    @Test
    void doesNotInteractWithCurrentlyTrackedSubjects() {
      var auths = new ClientUserAuths(null);

      var firstSubjectWithGivenId = mock(Subject.class);
      var secondSubjectWithGivenId = mock(Subject.class);
      var thirdSubjectWithGivenId = mock(Subject.class);
      var subjectWithOtherId1 = mock(Subject.class);
      var subjectWithOtherId2 = mock(Subject.class);

      var givenId = 34149L;
      auths.putSubject(firstSubjectWithGivenId, givenId);
      auths.putSubject(secondSubjectWithGivenId, givenId);
      auths.putSubject(thirdSubjectWithGivenId, givenId);
      auths.putSubject(subjectWithOtherId1, givenId + 9);
      auths.putSubject(subjectWithOtherId2, givenId + 22);

      auths.putSubject(mock(Subject.class), givenId);

      verifyNoInteractions(
          firstSubjectWithGivenId,
          secondSubjectWithGivenId,
          thirdSubjectWithGivenId,
          subjectWithOtherId1,
          subjectWithOtherId2);
    }

    @Test
    void doesNotConsumeAnId() {
      var idGenerator = mock(SubjectIdGenerator.class);
      var auths = new ClientUserAuths(idGenerator);

      auths.putSubject(mock(Subject.class), 98374L);

      verifyNoInteractions(idGenerator);
    }
  }

  @Nested
  class GetSubjectById {
    @Test
    void returnsNullIfNoSubjectIsAssociatedWithId() {
      var auths = new ClientUserAuths(null);

      var subjectId = 9234L;

      auths.putSubject(mock(Subject.class), 92L); // Different ID #1
      auths.putSubject(mock(Subject.class), 923L); // Different ID #2
      auths.putSubject(mock(Subject.class), 92345L); // Different ID #3
      // Note: We did not add a subject with subjectId

      assertThat(auths.getSubject(subjectId))
          .isNull();
    }

    @Test
    void returnsTheSubjectAssociatedWithId() {
      var auths = new ClientUserAuths(null);

      var subjectId = 238947L;
      var differentId1 = 23894L;
      var differentId2 = 2389L;
      var differentId3 = 238L;

      var subjectAssociatedWithId = mock(Subject.class);
      auths.putSubject(subjectAssociatedWithId, subjectId);

      auths.putSubject(mock(Subject.class), differentId1);
      auths.putSubject(mock(Subject.class), differentId2);
      auths.putSubject(mock(Subject.class), differentId3);

      assertThat(auths.getSubject(subjectId))
          .isSameAs(subjectAssociatedWithId);
    }

    @Test
    void returnsTheSubjectMostRecentlyAssociatedWithId() {
      var auths = new ClientUserAuths(null);

      var subjectId = 927834L;
      auths.putSubject(mock(Subject.class), subjectId);
      auths.putSubject(mock(Subject.class), subjectId);
      auths.putSubject(mock(Subject.class), subjectId);
      auths.putSubject(mock(Subject.class), subjectId);
      auths.putSubject(mock(Subject.class), subjectId);
      auths.putSubject(mock(Subject.class), subjectId);

      var mostRecentlyAddedWithSpecifiedId = mock(Subject.class);
      auths.putSubject(mostRecentlyAddedWithSpecifiedId, subjectId);

      assertThat(auths.getSubject(subjectId))
          .isSameAs(mostRecentlyAddedWithSpecifiedId);
    }
  }

  @Nested
  class RemoveSubject {
    @Test
    void removesAssociationOfAllSubjectsWithId() {
      var auths = new ClientUserAuths(null);

      var givenId = -487023L;
      auths.putSubject(mock(Subject.class), givenId);
      auths.putSubject(mock(Subject.class), givenId);
      auths.putSubject(mock(Subject.class), givenId);
      auths.putSubject(mock(Subject.class), givenId);
      auths.putSubject(mock(Subject.class), givenId);

      auths.removeSubject(givenId);

      assertThat(auths.getSubject(givenId))
          .isNull();
    }

    @Test
    void logsOutAllSubjectsAssociatedWithId() {
      var auths = new ClientUserAuths(null);

      var givenId = 8963947L;
      var subject1 = mock(Subject.class);
      var subject2 = mock(Subject.class);
      var subject3 = mock(Subject.class);
      var subject4 = mock(Subject.class);
      var subject5 = mock(Subject.class);

      auths.putSubject(subject1, givenId);
      auths.putSubject(subject2, givenId);
      auths.putSubject(subject3, givenId);
      auths.putSubject(subject4, givenId);
      auths.putSubject(subject5, givenId);

      auths.removeSubject(givenId);

      verify(subject1).logout();
      verify(subject2).logout();
      verify(subject3).logout();
      verify(subject4).logout();
      verify(subject5).logout();
    }

    @Test
    void retainsAssociationsOfAllSubjectsWithOtherIds() {
      var auths = new ClientUserAuths(null);

      var doomedSubjectId = 98347L;
      var retainedSubject1Id = doomedSubjectId + 1;
      var retainedSubject2Id = doomedSubjectId + 2;
      var retainedSubject3Id = doomedSubjectId + 3;

      var retainedSubject1 = mock(Subject.class);
      var retainedSubject2 = mock(Subject.class);
      var retainedSubject3 = mock(Subject.class);

      auths.putSubject(retainedSubject1, retainedSubject1Id);
      auths.putSubject(retainedSubject2, retainedSubject2Id);
      auths.putSubject(retainedSubject3, retainedSubject3Id);

      auths.putSubject(mock(Subject.class), doomedSubjectId);

      auths.removeSubject(doomedSubjectId);

      assertThat(auths.getSubject(retainedSubject1Id))
          .isSameAs(retainedSubject1);
      assertThat(auths.getSubject(retainedSubject2Id))
          .isSameAs(retainedSubject2);
      assertThat(auths.getSubject(retainedSubject3Id))
          .isSameAs(retainedSubject3);
    }

    @Test
    void doesNotInteractWithSubjectsAssociatedWithOtherIds() {
      var auths = new ClientUserAuths(null);

      var doomedSubjectId = 78974L;

      var retainedSubjectId1 = doomedSubjectId + 1;
      var retainedSubjectId2 = doomedSubjectId + 2;
      var retainedSubjectId3 = doomedSubjectId + 3;
      var retainedSubject1a = mock(Subject.class);
      var retainedSubject1b = mock(Subject.class);
      var retainedSubject1c = mock(Subject.class);
      var retainedSubject2a = mock(Subject.class);
      var retainedSubject2b = mock(Subject.class);
      var retainedSubject2c = mock(Subject.class);
      var retainedSubject3a = mock(Subject.class);
      var retainedSubject3b = mock(Subject.class);
      var retainedSubject3c = mock(Subject.class);

      auths.putSubject(retainedSubject1a, retainedSubjectId1);
      auths.putSubject(retainedSubject1b, retainedSubjectId1);
      auths.putSubject(retainedSubject1c, retainedSubjectId1);
      auths.putSubject(retainedSubject2a, retainedSubjectId2);
      auths.putSubject(retainedSubject2b, retainedSubjectId2);
      auths.putSubject(retainedSubject2c, retainedSubjectId2);
      auths.putSubject(retainedSubject3a, retainedSubjectId3);
      auths.putSubject(retainedSubject3b, retainedSubjectId3);
      auths.putSubject(retainedSubject3c, retainedSubjectId3);

      auths.putSubject(mock(Subject.class), doomedSubjectId);

      auths.removeSubject(doomedSubjectId);

      verifyNoInteractions(
          retainedSubject1a,
          retainedSubject1b,
          retainedSubject1c,
          retainedSubject2a,
          retainedSubject2b,
          retainedSubject2c,
          retainedSubject3a,
          retainedSubject3b,
          retainedSubject3c);
    }

    @Test
    void returnsWithoutThrowingIfNoSubjectsAssociatedWithId() {
      var auths = new ClientUserAuths(null);

      assertThatNoException().isThrownBy(() -> auths.removeSubject(5678L));
    }
  }

  @Nested
  class ContinuousQueryAuths {
    @Test
    void setUserAttributesForCqAssociatesIdentifiedSubjectWithCqName() {
      var auths = new ClientUserAuths(null);

      var id = 39874L;
      var cqName = "cq4132";
      var subject = mock(Subject.class);
      auths.putSubject(subject, id);

      auths.setUserAuthAttributesForCq(cqName, id, true);

      assertThat(auths.getSubject(cqName))
          .isSameAs(subject);
    }

    @Test
    void removeSubjectRemovesAssociationWithCqName() {
      var auths = new ClientUserAuths(null);

      var id = 33L;
      var cqName = "cq23497" + id;
      auths.putSubject(mock(Subject.class), id);
      auths.setUserAuthAttributesForCq(cqName, id, true);

      auths.removeSubject(id);

      assertThat(auths.getSubject(cqName))
          .isNull();
    }

    @Test
    void removeUserAuthAttributesForCqRemovesAssociationOfSubjectToCqName() {
      var auths = new ClientUserAuths(null);

      var id = -43780L;
      var cqName = "cq98740";
      auths.putSubject(mock(Subject.class), id);
      auths.setUserAuthAttributesForCq(cqName, id, true);

      auths.removeUserAuthAttributesForCq(cqName, true);

      assertThat(auths.getSubject(cqName))
          .isNull();
    }
  }
}
